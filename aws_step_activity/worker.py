# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

import sys
from time import sleep
from typing import TYPE_CHECKING, Optional, Dict, Type
from types import TracebackType

from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions

from .internal_types import Jsonable, JsonableDict


import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError
from .util import CreateSession

import threading
from threading import Thread, Lock, Condition

import json
import uuid
import time
import traceback
import sys

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
  worker_name: str
  shutting_down: bool = False
  heartbeat_seconds: float
  max_task_total_seconds: Optional[float]
  default_task_handler_class_name = "aws_step_activity.AwsStepActivityTaskHandler"


  def __init__(
        self,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
        activity_name: Optional[str]=None,
        activity_arn: Optional[str]=None,
        worker_name: Optional[str]=None,
        heartbeat_seconds: float=20.0,
        max_task_total_seconds: Optional[float]=None,
      ):
    """Create a new worker associated with a specific AWS step function activity

    Args:
        session (Optional[Session], optional):
            An AWS session to use as basis for access to AWS. If None, a new basis session is created
            using other parameters.  In any case a new session will be created from the basis, to ensure
            thread safety for background requests. Defaults to None.
        aws_profile (Optional[str], optional):
            An AWS profile name to use for a new basis session. Ignored if session is provided. If
            None, the default profile is used. Defaults to None.
        aws_region (Optional[str], optional):
            The AWS region to use. Defaults to None.
        activity_name (Optional[str], optional): _description_. Defaults to None.
        activity_arn (Optional[str], optional): _description_. Defaults to None.
        worker_name (Optional[str], optional): _description_. Defaults to None.
        heartbeat_seconds (float, optional): _description_. Defaults to 20.0.
        max_task_total_seconds (Optional[float], optional): _description_. Defaults to None.

    Raises:
        RuntimeError: _description_
        RuntimeError: _description_
        RuntimeError: _description_
    """
    if activity_name is None and activity_arn is None:
      raise RuntimeError("Either activity_name or activity_arn must be provided")
    if worker_name is None:
      worker_name = f'{uuid.getnode():#016x}'
    self.worker_name = worker_name
    self.heartbeat_seconds = heartbeat_seconds
    self.max_task_total_seconds = max_task_total_seconds

    self.mutex = Lock()
    self.cv = Condition(self.mutex)

    if session is None:
      session = Session(profile_name=aws_profile, region_name=aws_region)

    self.session = session

    sfn = self.session.client('stepfunctions')
    self.sfn = sfn

    if activity_arn is None:
      activity_name_to_arn: Dict[str, str] = {}
      paginator = sfn.get_paginator('list_activities')
      page_iterator = paginator.paginate()
      for page in page_iterator:
        for act_desc in page['activities']:
          activity_name_to_arn[act_desc['name']] = act_desc['activityArn']
      activity_arn = activity_name_to_arn.get(activity_name, None)
      if activity_arn is None:
        raise RuntimeError(f"AWS stepfunctions activity name '{activity_name}' was not found")
    else:
      resp = sfn.describe_activity(activityArn=activity_arn)
      if activity_name is None:
        activity_name = resp['name']
      else:
        if activity_name != resp['name']:
          raise RuntimeError(f"AWS stepfunctions activity name '{activity_name}' does not match actual activity name '{resp['name']}' for ARN '{activity_arn}'")
    self.activity_name = activity_name
    self.activity_arn = activity_arn
    
  def create_handler(self, task: AwsStepActivityTask) -> AwsStepActivityTaskHandler:
    handler_class_name = task.data.get()
    

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
        task: AwsStepActivityTask,
        heartbeat_seconds:Optional[float]=None,
        max_total_seconds: Optional[float]=None
      ):
    """Runs a single AWS stepfunction activity task that has been dequeued, sends periodic
    heartbeats, and sends an appropriate success or failure completion message for the task.

    Args:
        task (AwsStepActivityTask): The active task descriptor that should be run. task.data contains the imput
                                    parameters.
        heartbeat_seconds (Optional[float], optional):
           The number of seconds between heartbeats. Ignored if 'heartbeat_seconds' is provided
           in the task data. If None, the value provided at AwsStepActivityWorker construction time is used.
           Defaults to None.
        max_total_seconds (Optional[float], optional):
           The maximum total number of seconds to run the task before sending a failure completion.
           Ignored if 'max_total_seconds' is provided in the task data. If None, the value provided
           at AwsStepActivityWorker construction time is used. If that is None, then there will be no limit
           to how long the task can run. Defaults to None.
    """

    try:
      heartbeat_seconds_final: Optional[float] = task.data.get('heartbeat_seconds', None)
      if heartbeat_seconds_final is None:
        heartbeat_seconds_final = heartbeat_seconds
      if heartbeat_seconds_final is None:
        heartbeat_seconds_final = self.heartbeat_seconds

      max_total_seconds_final: Optional[float]
      if 'max_total_seconds' in task.data:
        max_total_seconds_final = task.data['max_total_seconds']
      else:
        max_total_seconds_final = max_total_seconds
        if max_total_seconds_final is None:
          max_total_seconds_final = self.max_task_total_seconds

      with AwsStepActivityTaskContext(
            task,
            session=self.session,
            interval_seconds=heartbeat_seconds_final,
            max_total_seconds=max_total_seconds_final
          ) as ctx:
        # at this point, heartbdeats are automatically being sent by a background thread, until
        # we exit the context
        result = self.run_task_in_context(task, ctx)
        # If an exception was raised, exiting the context will send the failure message
        ctx.send_task_success(result)

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
      



