# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""Implementation of AwsSteActivityTaskHandler, the base class for AWS step function activity task handlers"""

from .logging import logger

import sys
import os
from time import sleep
from typing import Optional, Dict, Type, Any, TYPE_CHECKING
from types import TracebackType
from .internal_types import Jsonable, JsonableDict, SFNClient, S3Client

import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError
from .util import create_aws_session
from .s3_util import s3_download_folder, s3_upload_folder
import threading
from threading import Thread, Lock, Condition

import json
import uuid
import time
import traceback
import sys
import shutil

from .worker import AwsStepActivityWorker
from .task import AwsStepActivityTask
from .util import create_aws_session, full_type

class AwsStepActivityTaskHandler:
  """A task handler for a single dequeued task instance on an AWS stepfunction activity
  
  Implementations should subclass this class and implement run_in_context.
  """
  session_mutex: Lock
  cv: Condition
  worker: AwsStepActivityWorker
  task: AwsStepActivityTask
  session: Session
  sfn: SFNClient
  s3: S3Client
  background_thread: Optional[Thread] = None
  task_completed: bool = False
  start_time_ns: int
  end_time_ns: Optional[int] = None
  task_working_dir: str
  task_id: str

  def __init__(
        self,
        worker: AwsStepActivityWorker,
        task: AwsStepActivityTask,
        task_working_dir: str
      ):
    """Create a new task handler specific AWS step function activity task instance

    Args:
        worker (AwsStepActivityWorker):
            The worker that this task handler is running under.
        task (AwsStepActivityTask):
            The task descriptor as received from AWS.
        task_working_dir (str):
            The directory in which to run the task, and in which to place task artifacts
    """
    self.start_time_ns = time.monotonic_ns()
    self.session_mutex = Lock()
    self.cv = Condition(self.session_mutex)
    self.worker = worker
    self.task = task
    self.task_working_dir = os.path.abspath(task_working_dir)
    self.session = create_aws_session(worker.session)
    self.sfn = self.session.client('stepfunctions')
    self.s3 = self.session.client('s3')
    self.task_id = self.worker.get_task_id(task)

  @property
  def input_data(self) -> Jsonable:
    return self.task.data

  def run_in_context(self) -> JsonableDict:
    """Synchronously runs this AWS stepfunction activity task inside an already active context.

    This method should be overriden by a subclass to provite a custom activity implementation.

    Heartbeats are already taken care of by the active context, until this function returns. If a
    JsonableDict is successfully returned, it will be used as the successful completion value for
    the task.  If an exception is raised, it will be used as the failure indication for the task.

    Raises:
        Exception:  Any exception that is raised will be used to form a failure cpompletion message for the task.

    Returns:
        JsonableDict: The deserialized JSON successful completion value for the task.
    """
    raise RuntimeError(f"run_in_context() is not implemented by class {full_type(self)}")
  
  @property
  def task_output_dir(self) -> str:
    return os.path.join(self.task_working_dir, 'output')

  @property
  def task_input_dir(self) -> str:
    return os.path.join(self.task_working_dir, 'input')

  def full_run_in_context(self) -> JsonableDict:
    """Synchronously runs this AWS stepfunction activity task inside an already active context.

    This method should be overriden by a subclass to provite a custom activity implementation.

    Heartbeats are already taken care of by the active context, until this function returns. If a
    JsonableDict is successfully returned, it will be used as the successful completion value for
    the task.  If an exception is raised, it will be used as the failure indication for the task.

    Raises:
        Exception:  Any exception that is raised will be used to form a failure cpompletion message for the task.

    Returns:
        JsonableDict: The deserialized JSON successful completion value for the task.
    """
    os.makedirs(self.task_output_dir, exist_ok=True)
    result: Optional[JsonableDict] = None
    try:
      try:
        os.makedirs(self.task_input_dir, exist_ok=True)
        if 's3_inputs' in self.task.data:
          s3_download_folder(self.task.data['s3_inputs'], output_folder=self.task_input_dir, s3=self.s3)
        
        result = self.run_in_context()
      finally:
        if 's3_outputs' in self.task.data:
          s3_upload_folder(self.task.data['s3_outputs'], self.task_output_dir, s3=self.s3)
    finally:
      keep_task_dir: bool = self.task.data.get('keep_task_dir', False)
      if not keep_task_dir:
        if os.path.exists(self.task_working_dir):
          shutil.rmtree(self.task_working_dir)

    return result

  def run(self):
    """Runs this stepfunction activity task, sends periodic
    heartbeats, and sends an appropriate success or failure completion message for the task.

    Args:
        task (AwsStepActivityTask): The active task descriptor that should be run. task.data contains the imput
                                    parameters.
    """
    try:
      with self:
        # at this point, heartbeats are automatically being sent by a background thread, until
        # we exit the context
        result = self.full_run_in_context()
        # If an exception was raised, exiting the context will send the failure message
        with self.session_mutex:
          if not self.task_completed:
            self.send_task_success_locked(result)
      # at this point, final completion has been sent and heartbeat has stopped
    except Exception as ex:
      try:
        with self.session_mutex:
          if not self.task_completed:
            exc_type, exc, tb = sys.exc_info()
            self.send_task_exception_locked(exc, tb=tb, exc_type=exc_type)
      except Exception:
        pass

  def fill_default_output_data(self, data: JsonableDict) -> None:
    if not 'run_time_ns' in data:
      data['run_time_ns'] = self.elapsed_time_ns()
    if not 'task_id' in data:
      data['task_id'] = self.task_id
    if not 's3_outputs' in data and 's3_outputs' in self.task.data:
      data['s3_outputs'] = self.task.data['s3_outputs']
  
  def fill_default_success_data(self, data: JsonableDict) -> None:
    self.fill_default_output_data(data)
    
  def fill_default_failure_data(self, data: JsonableDict) -> None:
    self.fill_default_output_data(data)
      
  @property
  def shutting_down(self) -> bool:
    """True if the worker that owns this task is shutting down"""
    return self.worker.shutting_down
  
  def check_for_cancelled(self):
    """Raises an exception if this task has already completed (due to cancellation or timeout),
    or if the worker that owns this task is shiutting down.

    Raises:
        RuntimeError: The task has been cancelled or the worker is shutting down.  The handler
                       should exit as soon as possible.
    """
    if self.task_completed or self.shutting_down:
      raise RuntimeError("Execution of the AwsStepActivityTaskHandler was cancelled")

  def send_task_success_locked(self, output_data: Optional[JsonableDict]=None):
    """Sends a successful completion notification with output data for the task.
       session_lock must already be held.

    Args:
        output_data (Optional[JsonableDict], optional):
          Deserialized JSON containing the successful results of the task. If None, an empty
          dict will be used.  Default fields will be added as appropriate.

    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    final_output_data = {} if output_data is None else dict(output_data)
    self.end_time_ns = time.monotonic_ns()
    self.fill_default_success_data(final_output_data)
    output_json = json.dumps(final_output_data, sort_keys=True, separators=(',', ':'))
    logger.debug(f"Sending task_success, output={json.dumps(final_output_data, sort_keys=True, indent=2)}")
    self.task_completed = True
    self.sfn.send_task_success(
        taskToken=self.task.task_token,
        output=output_json
      )
    self.cv.notify_all()
    self.on_complete_sent_locked()

  def send_task_success(self, output_data: Optional[JsonableDict]=None):
    """Sends a successful completion notification with output data for the task.
       session_lock must not be held.

    Args:
        output_data (Optional[JsonableDict], optional):
          Deserialized JSON containing the successful results of the task. If None, an empty
          dict will be used.  Default fields will be added as appropriate.

    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    with self.session_mutex:
      self.send_task_success_locked(output_data)

  def send_task_failure_locked(self, error: Any=None, cause: Jsonable=None):
    """Sends a failure completion notification for the task.
       session_lock must already be held.

    Args:
        error (Any, optional):
          Any value that can be converted to a string to describe the error that cause failure. If None, a generic
          error string is provided.  The resulting string will be truncated to 256 characters.
        cause (Jsonable, optional):
          Any deserialized JSON value that serves to describe the cause of the failure. If not a dict, a
          dict will be created and the value of this parameter will be assigned to the 'value' property.
          Default fields will be added as appropriate.
          
    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    if error is None:
      error_str = "The activity task failed"
    else:
      error_str = str(error)
    if len(error_str) > 256:
      error_str = error_str[:256]   # AWS constraint
    if cause is None:
      cause = {}
    elif isinstance(cause, dict):
      cause = dict(cause)
    else:
      cause = dict(value=cause)
    self.end_time_ns = time.monotonic_ns()
    self.fill_default_failure_data(cause)
    cause_str = json.dumps(cause, sort_keys=True, separators=(',', ':'))
    logger.info(f"Sending task_failure, error='{error_str}', cause={json.dumps(cause, indent=2, sort_keys=True)}")
    self.task_completed = True
    self.sfn.send_task_failure(
        taskToken=self.task.task_token,
        cause=cause_str,
        error=error_str
      )
    self.cv.notify_all()
    self.on_complete_sent_locked()

  def send_task_failure(self, error: Any=None, cause: Jsonable=None):
    """Sends a failure completion notification for the task.
       session_lock must not be held.

    Args:
        error (Any, optional):
          Any value that can be converted to a string to describe the error that cause failure. If None, a generic
          error string is provided.  The resulting string will be truncated to 256 characters.
        cause (Jsonable, optional):
          Any deserialized JSON value that serves to describe the cause of the failure. If not a dict, a
          dict will be created and the value of this parameter will be assigned to the 'value' property.
          Default fields will be added as appropriate.
          
    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    with self.session_mutex:
      self.send_task_failure_locked(error=error, cause=cause)

  def send_task_exception_locked(
        self,
        exc: BaseException,
        tb: Optional[TracebackType]=None,
        exc_type: Optional[Type[BaseException]]=None,
      ):
    """Sends a failure completion notification based on an exception for the task.
       session_lock must already be held.

    Args:
        exc (BaseException):
          The exception that caused failure of the task.
        tb (Optional[TracebackType], optional):
          An optional stack trace that will be included in the cause of the failure.
        exc_type (Optional[Type[BaseException]], optional):
          The class of exception being reported.  If None, the type of exc will be used.
          
    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    if exc_type is None:
      exc_type = type(exc)
    tb_list = traceback.format_exception(etype=exc_type, value=exc, tb=tb, limit=20)
    cause: JsonableDict = dict(tb=tb_list)
    self.send_task_failure_locked(error=exc, cause=cause)

  def send_task_exception(
        self,
        exc: BaseException,
        tb: Optional[TracebackType]=None,
        exc_type: Optional[Type[BaseException]]=None,
      ):
    """Sends a failure completion notification based on an exception for the task.
       session_lock must not be held.

    Args:
        exc (BaseException):
          The exception that caused failure of the task.
        tb (Optional[TracebackType], optional):
          An optional stack trace that will be included in the cause of the failure.
        exc_type (Optional[Type[BaseException]], optional):
          The class of exception being reported.  If None, the type of exc will be used.
          
    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    with self.session_mutex:
      self.send_task_exception_locked(exc, tb=tb, exc_type=exc_type)

  def send_task_heartbeat_locked(self):
    """Sends a heartbeat keepalive notification for the task.
       session_lock must already be held.
    
       This prevents AWS from timing out the task before it has been completed.

    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    logger.debug(f"Sending task_heartbeat")
    self.sfn.send_task_heartbeat(
        taskToken=self.task.task_token
      )
    logger.debug(f"task_heartbeat sent successfully")

  def send_task_heartbeat(self):
    """Sends a heartbeat keepalive notification for the task.
       session_lock must not be held.
    
       This prevents AWS from timing out the task before it has been completed.

    Raises:
        RuntimeError: Success or failure notification for the task has already been sent.
    """
    with self.session_mutex:
      self.send_task_heartbeat_locked()

  def __enter__(self) -> 'AwsStepActivityTaskHandler':
    """Enters a context that ensures that the task will be kept alive
       with heartbeets and that a single success or failure notification will
       be sent for the task by the time the context exits.
       
       session_mutex must not be held by the caller.
       
       Starts a background thread that sends heartbeats.
       
       On exit of the context, if no completion has been sent, then
       sends a failure completion if an Exception was raised; otherwise
       sends a successful completion.
       
    Example:
    
        worker = AwsStepActivityWorker(,,,)
        task = worker.get_next_task()
        

    Raises:
        RuntimeError: The context has already been entered

    Returns:
        AwsStepActivityTaskHandler: This task handler
    """
    logger.debug(f"Entering task context")
    with self.session_mutex:
      if not self.background_thread is None:
        raise RuntimeError("AwsStepActivityTaskHandler.__enter__: Context already entered")
      background_thread = Thread(target=lambda: self.background_fn())
      self.background_thread = background_thread
      try:
        background_thread.start()
      except Exception as ex:
        self.background_thread = None
        try:
          self.send_task_exception_locked(ex)
        except Exception:
          pass
        raise
    logger.debug(f"Task context entered")
    return self

  def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType]
      ) -> Optional[bool]:
    """Exits the context that performs heartbeats and ensures a single task completion is sent
    
    Shuts down the background thread that sends heartbeats.
    
    If no completion has been sent and an Exception caused the context to exit, then sends
    a failure notification based on the Exception.
    
    If no completion has been sent and no Exception caused the context to exit, then sends
    a generic success completion notifiocation.

    Args:
        exc_type (Optional[Type[BaseException]]):
            The type of Exception that caused the context to exit, or None if there was
            no Exception.
        exc (Optional[BaseException]):
            The Exception that caused the context to exit, or None if there was
            no Exception.
        tb (Optional[TracebackType]):
           An optional stack trace for the exception that caused the context to exit,
           or None if there is no traceback.

    Returns:
        Optional[bool]:
            True if the context completed successfully. False if an exception caused the
            context to exit (this will result in the execption being propagated outside
            the context).
    """
    logger.debug(f"Exiting task context")
    background_thread: Optional[Thread] = None
    with self.session_mutex:
      background_thread = self.background_thread
      self.background_thread = None
      if not self.task_completed:
        if exc is None:
          self.send_task_success_locked(output_data={})
        else:
          self.send_task_exception_locked(exc, tb=tb, exc_type=exc_type)
    if not background_thread is None:
      background_thread.join()
    logger.debug(f"Task context exited")
    return exc_type is None

  def elapsed_time_ns(self) -> int:
    """Returns the elapsed nanoseconds since the task was started.
    
       After the task is completed successfully or with failure, the
       elapsed time is frozen and will always return the same value

    Returns: Optional[float]:
        If the task is completed, the total runtime in nanoseconds.
        Otherwise, the current task runtime in nanoseconds.
    """
    end_time_ns = self.end_time_ns
    if end_time_ns is None:
      end_time_ns = time.monotonic_ns()
    return end_time_ns - self.start_time_ns
  
  def elapsed_time_seconds(self) -> float:
    """Returns the elapsed seconds since the task was started.
    
       After the task is completed successfully or with failure, the
       elapsed time is frozen and will always return the same value

    Returns: Optional[float]:
        If the task is completed, the total runtime in seconds.
        Otherwise, the current task runtime in seconds.
    """
    return self.elapsed_time_ns() / 1000000000.0

  def remaining_time_ns(self) -> Optional[int]:
    """Returns the remaining time in nanoseconds before the task is cancelled
    
       Returns None if there is no time limit.

    Returns:
        Optional[int]: Remaining time in nanoseconds, or None if there is no limit.
    """
    remaining_secs = self.remaining_time_seconds()
    return None if remaining_secs is None else round(remaining_secs * 1000000000.0)

  def remaining_time_seconds(self) -> Optional[float]:
    """Returns the remaining time in seconds before the task is cancelled
    
       Returns None if there is no time limit.

    Returns:
        Optional[float]: Remaining time in nanoseconds, or
    """
    max_total_secs = self.max_runtime_seconds()
    if max_total_secs is None or max_total_secs <= 0:
      return None
    return max(0.0, max_total_secs - self.elapsed_time_seconds())

  def heartbeat_interval_seconds(self) -> float:
    """Returns the final resolved heartbeat interval for the task in seconds"""
    heartbeat_seconds_final: Optional[float] = None
    if 'heartbeat_seconds' in self.task.data:
       heartbeat_seconds_final = self.task.data['heartbeat_seconds']
    if heartbeat_seconds_final is None:
      heartbeat_seconds_final = self.worker.heartbeat_seconds
    heartbeat_seconds_final = float(heartbeat_seconds_final)
    return heartbeat_seconds_final

  def max_runtime_ns(self) -> Optional[int]:
    """Returns the final resolved maximum runtime for the task in nanoseconds, or None if there is no limit"""
    max_secs = self.max_runtime_seconds()
    return None if max_secs is None else round(max_secs * 1000000000.0)
    
  def max_runtime_seconds(self) -> Optional[float]:
    """Returns the final resolved maximum runtime for the task in seconds, or None if there is no limit"""
    max_total_seconds_final: Optional[float]
    if 'max_total_seconds' in self.task.data:
      max_total_seconds_final = self.task.data['max_total_seconds']
    else:
      max_total_seconds_final = self.worker.max_task_total_seconds
    if not max_total_seconds_final is None:
      max_total_seconds_final = float(max_total_seconds_final)
    return max_total_seconds_final
    
  def background_fn(self):
    """A background thread that periodically sends heartbeat keepalive messages for the task
    
    This function runs in its own thread. It is started when the context is entered, and
    shut down when a successful or failure notification is sent for the task, or when
    the max runtime for the task is exceeded (in this case, this function will send
    final failure notification).
    
    """
    logger.debug(f"Background thread starting")
    with self.session_mutex:
      try:
        heartbeat_seconds = self.heartbeat_interval_seconds()
        while True:
          if self.task_completed:
            break
          # We will go to sleep for the minimum of the heartbeat interval
          # and the max remaining runtime for the task. If the main thread completes
          # the task, they will wake us up early.
          sleep_secs = heartbeat_seconds
          remaining_time_sec = self.remaining_time_seconds()
          if not remaining_time_sec is None:
            if remaining_time_sec <= 0.0:
              raise RuntimeError("Max task runtime exceeded")
            if remaining_time_sec < sleep_secs:
              sleep_secs = remaining_time_sec
          logger.debug(f"Background thread sleeping for {sleep_secs} seconds")
          # Waiting on the condition variable will temporarily release
          # session_mutex, which will allow the main thread to send
          # success or failure notifications while we are sleeping. If they
          # do that, they will wake us up early and we will find out
          # the task is complete and exit
          self.cv.wait(timeout=sleep_secs)
          logger.debug(f"Background thread awake")
          if self.task_completed:
            # The main thread completed the task
            break
          remaining_time_sec = self.remaining_time_seconds()
          if not remaining_time_sec is None and remaining_time_sec <= 0.0:
            # Task has run out of time. Send a failure notification and exit early.
            raise RuntimeError("Max task runtime exceeded")
          self.send_task_heartbeat_locked()
      except Exception as ex:
        logger.warning(f"Exception in background thread: {ex}")
        try:
          self.send_task_exception_locked(ex)
        except Exception:
          pass
    logger.debug(f"Background thread exiting")

  def on_complete_sent_locked(self):
    """Called after a completion notification is sent
    
    session_lock is held. May be called from any thread.  Guaranteed to only be called once.
    
    May be overridden by subclasses to cancel long-running processes if the
    task is cancelled or times out before running to completion
    """
    pass
