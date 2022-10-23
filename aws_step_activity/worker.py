# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""Implementation of AwsStepActivityWorker"""

from .logging import logger

import sys
from time import sleep
from typing import TYPE_CHECKING, Optional, Dict, Type, Union
from types import TracebackType

from .internal_types import Jsonable, JsonableDict, SFNClient

import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError
from .util import create_aws_session, full_type
from .sfn_util import describe_aws_step_activity

import threading
from threading import Thread, Lock, Condition

import json
import uuid
import time
import traceback
import hashlib
import os
import sys
from datetime import datetime
from dateutil.parser import parse as dateutil_parse

from .constants import DEFAULT_AWS_STEP_ACTIVITY_TASK_HANDLER_CLASS_NAME
from .task import AwsStepActivityTask

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
  task_working_dir_parent: str

  def __init__(
        self,
        activity_id: str,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
        worker_name: Optional[str]=None,
        heartbeat_seconds: float=20.0,
        max_task_total_seconds: Optional[float]=None,
        default_task_handler_class: Optional[Union[str, Type['AwsStepActivityTaskHandler']]]=None,
        task_working_dir_parent: Optional[str] = None
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
        default_task_handler_class(Optional[Union[str, Type[AwsStepActivityTaskHandler]]], optional):
            A subclass of AwsStepActivityTaskHandler, or the fully qualified name of such a subclass. If
            None, DEFAULT_AWS_STEP_ACTIVITY_TASK_HANDLER_CLASS_NAME will be used. Each time a task
            is dequeued from AWS, an instance of this class will be created to handle the task.
        task_working_dir_parent(Optional[str], optional):
            The parent directory under which task working directories should be created. If None,
            './tasks' is used
    """
    if worker_name is None:
      worker_name = f'{uuid.getnode():016x}'
    self.worker_name = worker_name
    self.heartbeat_seconds = heartbeat_seconds
    self.max_task_total_seconds = max_task_total_seconds
    self.default_task_handler_class = self.resolve_handler_class(default_task_handler_class)
    if task_working_dir_parent is None:
      task_working_dir_parent = 'tasks'
    self.task_working_dir_parent = os.path.abspath(task_working_dir_parent)

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

  def get_task_id(self, task: AwsStepActivityTask) -> str:
    task_token = task.task_token
    task_id = hashlib.sha256(task_token.encode('utf-8')).hexdigest()
    return task_id

  def get_task_working_dir(self, task: AwsStepActivityTask) -> str:
    task_id = self.get_task_id(task)
    twd = os.path.join(self.task_working_dir_parent, task_id)
    return twd

  def create_handler(self, task: AwsStepActivityTask) -> 'AwsStepActivityTaskHandler':
    handler_class = self.resolve_handler_class(task.data.get('handler_class', None))
    task_working_dir = self.get_task_working_dir(task)
    handler = handler_class(self, task, task_working_dir)
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
      logger.debug(f"Waiting for AWS step function activity task on ARN={self.activity_arn}, name={self.activity_name}")
      resp = self.sfn.get_activity_task(activityArn=self.activity_arn, workerName=self.worker_name)
    except ReadTimeoutError:
      return None
    if not 'taskToken' in resp:
      return None
    return AwsStepActivityTask(resp)

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

    task_id: Optional[str] = None
    try:
      task_id = self.get_task_id(task)
      logger.info(f"Beginning AWS stepfunction task {task_id}")
      logger.info(f"AWS stepfunction data = {json.dumps(task.data, indent=2, sort_keys=True)}")
      handler = self.create_handler(task)
      logger.debug(f"AWS stepfunction activity handler class = {full_type(handler)}")
      handler.run()
    except Exception as ex:
      try:
        handler = AwsStepActivityTaskHandler(self, task)
        exc, exc_type, tb = sys.exc_info()
        handler.send_task_exception(exc, tb=tb, exc_type=exc_type)
      except Exception as ex2:
        logger.warning(f"Unable to send generic failure response ({ex}) for task: {ex2}")
    except Exception as e:
      logger.info(f"Exception occurred processing AWS step function activity task {task_id}")
      logger.info(traceback.format_exc())
    logger.info(f"Completed AWS stepfunction task {task_id}")

  def run(self):
    """Repeatedly wait for and dequeue AWS stepfunction activity tasks and run them"""
    logger.info(
        f"Starting AWS step function activity worker on "
          f"activity ARN='{self.activity_arn}', "
          f"activity name='{self.activity_name}', "
          f"worker name='{self.worker_name}'"
      )
    while not self.shutting_down:
      task = self.get_next_task()
      if task is None:
        logger.debug(f"AWS stepfunctions.get_next_task(activity_arn='{self.activity_arn}') long poll timed out... retrying")
      else:
        self.run_task(task)
    logger.info(f"Stopping AWS step function activity worker, name='{self.worker_name}")

  def shutdown(self):
    self.shutting_down = True
      



