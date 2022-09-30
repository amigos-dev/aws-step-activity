# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

import sys
from time import sleep
from typing import Optional, Dict, Type, Any
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

from .worker import AwsStepActivityWorker
from .task import AwsStepActivityTask
from .task_context import AwsStepActivityTaskContext
from .util import CreateSession

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
  background_thread: Optional[Thread] = None
  heartbeat_seconds: Optional[float]
  max_total_seconds: Optional[float]
  task_completed: bool = False
  start_time_ns: int
  end_time_ns: Optional[int] = None

  def __init__(
        self,
        worker: AwsStepActivityWorker,
        task: AwsStepActivityTask,
        heartbeat_seconds:Optional[float]=None,
        max_total_seconds: Optional[float]=None
      ):
    """Create a new task handler specific AWS step function activity task instance

    Args:
        worker (AwsStepActivityWorker):
            The worker that this task handler is running under.
        task (AwsStepActivityTask):
            The task descriptor as received from AWS.
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
    self.start_time_ns = time.monotonic_ns()
    self.session_mutex = Lock()
    self.cv = Condition(self.session_mutex)
    self.worker = worker
    self.task = task
    self.session = CreateSession(worker.session)
    self.sfn = self.session.client('stepfunctions')
    self.heartbeat_seconds = heartbeat_seconds
    self.max_total_seconds = max_total_seconds

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
    time.sleep(10)
    raise RuntimeError("run_in_context() is not implemented")

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
        result = self.run_in_context()
        # If an exception was raised, exiting the context will send the failure message
        self.send_task_success(result)
      # at this point, final completion has been sent and heartbeat has stopped
    except Exception as ex:
      try:
        print(f"Exception occurred processing AWS step function activity task {self.task.task_token}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
      except Exception:
        pass
      # if an exception was raised before entering the context, then no final completion was sent
      try:
        with self.session_mutex:
          if not self.task_completed:
            self.send_task_exception_locked(ex)
      except Exception:
        pass

  def fill_default_output_data(self, data: JsonableDict) -> None:
    if not 'run_time_ns' in data
      data['run_time_ns'] = self.elapsed_time_ns()
  
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
    print(f"Sending task_success, output={json.dumps(final_output_data, sort_keys=True, indent=2)}", file=sys.stderr)
    self.task_completed = True
    self.sfn.send_task_success(
        taskToken=self.task.task_token,
        output=output_json
      )
    self.cv.notify_all()

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
    print(f"Sending task_failure, error='{error_str}', cause={json.dumps(cause, indent=2, sort_keys=True)}", file=sys.stderr)
    self.task_completed = True
    self.sfn.send_task_failure(
        taskToken=self.task.task_token,
        cause=cause_str,
        error=error_str
      )
    self.cv.notify_all()

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
    print(f"Sending task_heartbeat", file=sys.stderr)
    self.sfn.send_task_heartbeat(
        taskToken=self.task.task_token
      )
    print(f"task_heartbeat sent successfully", file=sys.stderr)

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
    print(f"Entering task context", file=sys.stderr)
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
    print(f"Task context entered", file=sys.stderr)
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
    print(f"Exiting task context", file=sys.stderr)
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
    print(f"Task context exited", file=sys.stderr)
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
    return self.elapsed_time_ns()) / 1000000000.0

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
    """Returns the final resolved heartbeat interval fopr the task in seconds"""
    heartbeat_seconds_final: Optional[float] = None
    if 'heartbeat_seconds' in self.task.data:
       heartbeat_seconds_final = = self.task.data['heartbeat_seconds']
    if heartbeat_seconds_final is None:
      heartbeat_seconds_final = self.heartbeat_seconds
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
      max_total_seconds_final = self.task.data['max_total_seconds'])
    else:
      max_total_seconds_final = self.max_total_seconds
      if max_total_seconds_final is None:
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
    print(f"Background thread starting", file=sys.stderr)
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
          print(f"Background thread sleeping for {sleep_secs} seconds", file=sys.stderr)
          # Waiting on the condition variable will temporarily release
          # session_mutex, which will allow the main thread to send
          # success or failure notifications while we are sleeping. If they
          # do that, they will wake us up early and we will find out
          # the task is complete and exit
          self.cv.wait(timeout=sleep_secs)
          print(f"Background thread awake", file=sys.stderr)
          if self.task_completed:
            # The main thread completed the task
            break
          remaining_time_sec = self.remaining_time_seconds()
          if not remaining_time_sec is None and remaining_time_sec <= 0.0:
            # Task has run out of time. Send a failure notification and exit early.
            raise RuntimeError("Max task runtime exceeded")
          self.send_task_heartbeat_locked()
      except Exception as ex:
        print(f"Exception in background thread: {ex}", file=sys.stderr)
        try:
          self.send_task_exception_locked(ex)
        except Exception:
          pass
    print(f"Background thread exiting", file=sys.stderr)
