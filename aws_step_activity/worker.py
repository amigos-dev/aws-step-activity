# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

import sys
from time import sleep
from typing import TYPE_CHECKING, Optional, Dict, Type, Union
from types import TracebackType

from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions

from .internal_types import Jsonable, JsonableDict


import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError
from .util import create_aws_session, describe_aws_step_activity

import threading
from threading import Thread, Lock, Condition

import json
import uuid
import time
import traceback
import sys
from datetime import datetime
from dateutil.parser import parse as dateutil_parse

from .constants import DEFAULT_AWS_STEP_ACTIVITY_TASK_HANDLER_CLASS_NAME
from .task import AwsStepActivityTask
from .task_context import AwsStepActivityTaskContext

if TYPE_CHECKING:
  from .handler import AwsStepActivityTaskHandler
  
class AwsStepActivityWorker:
  mutex: Lock
  cv: Condition
  session: Session
  sfn: SFNClient
  activity_name: str
  activity_arn: str
  activity_creation_date: datetime
  worker_name: str
  shutting_down: bool = False
  heartbeat_seconds: float
  max_task_total_seconds: Optional[float]
  default_task_handler_class: Optional[Type['AwsStepActivityTaskHandler']] = None

  def __init__(
        self,
        activity_id: str,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
        worker_name: Optional[str]=None,
        heartbeat_seconds: float=20.0,
        max_task_total_seconds: Optional[float]=None,
        default_task_handler_class: Optional[Union[str, Type['AwsStepActivityTaskHandler']]]=None
      ):
    """Create a new worker associated with a specific AWS step function activity

    Args:
        activity_id (str):
            The ARN or the name of the AWS stepfunction activity.
        session (Optional[Session], optional):
            An AWS session to use as basis for access to AWS. If None, a new basis session is created
            using other parameters.  In any case a new session will be created from the basis, to ensure
            thread safety for background requests. Defaults to None.
        aws_profile (Optional[str], optional):
            An AWS profile name to use for a new basis session. Ignored if session is provided. If
            None, the default profile is used. Defaults to None.
        aws_region (Optional[str], optional):
            The AWS region to use for creation of a new session. Ignored if session is provided. If None,
            the default region for the AWS profile is used. Defaults to None.
        worker_name (Optional[str], optional):
            The name of this worker node, for use in logging and completion reporting. If None,
            a unique name based on the local MAC address is created. Defaults to None.
        heartbeat_seconds (float, optional):
            The default number of seconds between heartbeat notifications to AWS while a task execution is in
            progress.  Ignored if a particular task has heartbeat_seconds provided in the task parameters, or
            if heartbeat_seconds is provided ad run() time. Defaults to 20.0.
        max_task_total_seconds (Optional[float], optional):
            The default maximum total number of seconds for a dask to run before a failure is posted. None
            or 0.0 if no limit is to be imposed. Ignored if max_task_total_seconds is provided in the task
            parameters, or if max_task_total_seconds is provided at run() time. Defaults to None.
    """
    if worker_name is None:
      worker_name = f'{uuid.getnode():#016x}'
    self.worker_name = worker_name
    self.heartbeat_seconds = heartbeat_seconds
    self.max_task_total_seconds = max_task_total_seconds
    self.default_task_handler_class = self.resolve_handler_class(default_task_handler_class)

    self.mutex = Lock()
    self.cv = Condition(self.mutex)

    if session is None:
      session = Session(profile_name=aws_profile, region_name=aws_region)

    self.session = session

    sfn = self.session.client('stepfunctions')
    self.sfn = sfn

    resp = describe_aws_step_activity(sfn, activity_id)

    self.activity_arn: str = resp['activityArn']
    self.activity_name: str = resp['name']
    self.activity_creation_date = dateutil_parse(resp['creationDate'])
    
  def resolve_handler_class(
        self,
        handler_class: Optional[Union[str, Type['AwsStepActivityTaskHandler']]]) -> Type['AwsStepActivityTaskHandler']:
    from .handler import AwsStepActivityTaskHandler
    if handler_class is None:
      handler_class = self.default_task_handler_class
    if handler_class is None:
      handler_class = DEFAULT_AWS_STEP_ACTIVITY_TASK_HANDLER_CLASS_NAME
    if isinstance(handler_class, str):
      import importlib
      from .handler import AwsStepActivityTaskHandler
      module_name, short_class_name = handler_class.rsplit('.', 1)
      module = importlib.import_module(module_name)
      handler_class = getattr(module, short_class_name)
    if not issubclass(handler_class, AwsStepActivityTaskHandler):
      raise RuntimeError(f"handler class is not a subclass of AwsStepActivityTaskHandler: {handler_class}")
    return handler_class
    
  def create_handler(self, task: AwsStepActivityTask) -> 'AwsStepActivityTaskHandler':
    handler_class = self.resolve_handler_class(task.data.get('handler_class', None))
    handler = handler_class(self, task)
    return handler

  def get_next_task(self) -> Optional[AwsStepActivityTask]:
    """Use long-polling to wait for and dequeue the next task on the AWS stepfunctions activity.

    This call may block for up to several minutes waiting for a task to become available. If
    no task is successfully dequeued after theh long-poll time limit expires, then None is returned.

    If a task is successfully dequeued, the caller MUST make a best effort to send periodic heartbeats
    to the task, and send a final success/failure message to the task.

    Returns:
        Optional[AwsStepActivityTask]: The dequeued task descriptor, or None if no task was dequeued.
    """
    try:
      print(f"Waiting for AWS step function activity task on ARN={self.activity_arn}, name={self.activity_name}", file=sys.stderr)
      resp = self.sfn.get_activity_task(activityArn=self.activity_arn, workerName=self.worker_name)
    except ReadTimeoutError:
      return None
    if not 'taskToken' in resp:
      return None
    return AwsStepActivityTask(resp)

  def run_task_in_context(self, task: AwsStepActivityTask, ctx: AwsStepActivityTaskContext) -> JsonableDict:
    """Synchronously runs a single AWS stepfunction activity task that has been dequeued, inside an already active context.

    This method should be overriden by a subclass to provite a custom activity implementation.

    Heartbeats are already taken care of by the active context, until this function returns. If a
    JsonableDict is successfully returned, it will be used as the successful completion value for
    the task.  If an exception is raised, it will be used as the failure indication for the task.

    Args:
        task (AwsStepActivityTask): The active task descriptor that should be run. task.data contains the imput
                                    parameters.
        ctx (AwsStepActivityTaskContext):
                                    The already active context manager in which the task is running. Generally
                                    not needed, but it can be used to determine if the task was cancelled or
                                    timed out, or to send a custom failure response rather than raising an exception.

    Raises:
        Exception:  Any exception that is raised will be used to form a failure cpompletion message for the task.

    Returns:
        JsonableDict: The deserialized JSON successful completion value for the task.
    """
    time.sleep(10)
    raise RuntimeError("run_task_in_context() is not implemented")

  def run_task(
        self,
        task: AwsStepActivityTask
      ):
    """Runs a single AWS stepfunction activity task that has been dequeued, sends periodic
    heartbeats, and sends an appropriate success or failure completion message for the task.

    Args:
        task (AwsStepActivityTask): The active task descriptor that should be run. task.data contains the imput
                                    parameters.
    """
    from .handler import AwsStepActivityTaskHandler

    try:
      handler = self.create_handler(task)
      handler.run()
    except Exception as ex:
      try:
        handler = AwsStepActivityTaskHandler(self, task)
        exc, exc_type, tb = sys.exc_info()
        handler.send_task_exception(exc, tb=tb, exc_type=exc_type)
      except Exception as ex2:
        print(f"Unable to send generic failure response ({ex}) for task: {ex2}", file=sys.stderr)
    except Exception as e:
      print(f"Exception occurred processing AWS step function activity task {task.task_token}", file=sys.stderr)
      print(traceback.format_exc(), file=sys.stderr)

  def run(self):
    """Repeatedly wait for and dequeue AWS stepfunction tasks and run them"""
    while not self.shutting_down:
      task = self.get_next_task()
      if task is None:
        print(f"AWS stepfunctions.get_next_task(activity_arn='{self.activity_arn}') long poll timed out... retrying", file=sys.stderr)
      else:
        print(f"Beginning AWS stepfunction task {task.task_token}", file=sys.stderr)
        print(f"AWS stepfunction data = {json.dumps(task.data, indent=2, sort_keys=True)}", file=sys.stderr)
        self.run_task(task)
        print(f"Completed AWS stepfunction task {task.task_token}", file=sys.stderr)

  def shutdown(self):
    self.shutting_down = True
      



