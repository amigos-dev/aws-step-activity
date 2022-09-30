# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

from typing import Optional, Dict, Type, Any
from types import TracebackType
from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions

from .internal_types import Jsonable, JsonableDict

import boto3
from boto3 import Session
from .util import CreateSession

import threading
from threading import Thread, Lock, Condition

import traceback
import json
import time
import sys

from .task import AwsStepActivityTask

class AwsStepActivityTaskContext:
  """A context manager object that periodically sends keepalive heartbeat messages
  for an active AWS stepfunctions activity task, and posts final completion status.
  """
  session_mutex: Lock
  cv: Condition
  task: AwsStepActivityTask
  background_session: Session
  background_sfn: SFNClient
  background_thread: Optional[Thread] = None
  heartbeat_seconds: float
  max_total_seconds: float
  task_completed: bool = False
  start_time_ns: int
  end_time_ns: Optional[int] = None

  def __init__(
        self,
        task: AwsStepActivityTask,
        session: Optional[Session]=None,
        heartbeat_seconds: float=20.0,
        max_total_seconds: Optional[float]=None
      ):
    """Create a context manager for a running AWS stepfunction activity task

    When the context is entered, heartbeats will be sent for the task in the background until
    the context is exited or it fails.

    Example:
        with AwsStepActivityTaskHeartBeater(myTask, session=mySession, max_total_seconds=600.0):
          do_my_task()
          send_task_completion()

    Args:
        task (AwsStepActivityTask): The descriptor for the task
        session (Optional[Session], optional):
                The AWS session to use as a template, or None to use a default session.
                A new session will always be created from the template for thread safety. Defaults to None.
        heartbeat_seconds (float, optional):
                The interval in seconds between heartbeats. Defaults to 20.0.
        max_total_seconds (Optional[float], optional):
                A maximum number of total seconds before the task should fail. If None or 0.0, the task
                will never time out. Defaults to None.
    """
    self.start_time_ns = time.monotonic_ns()
    self.heartbeat_seconds = heartbeat_seconds
    if max_total_seconds is None:
      max_total_seconds = 0.0
    self.max_total_seconds = max_total_seconds
    self.task = task
    self.session_mutex = Lock()
    self.cv = Condition(self.session_mutex)

    if session is None:
      session = Session()

    self.background_session = CreateSession(session=session)
    self.background_sfn = self.background_session.client('stepfunctions')

  def send_task_success_locked(self, output_data: JsonableDict):
    output_json = json.dumps(output_data, sort_keys=True, separators=(',', ':'))
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    self.task_completed = True
    self.end_time_ns = time.monotonic_ns()
    print(f"Sending task_success, output={json.dumps(output_data, sort_keys=True, indent=2)}", file=sys.stderr)
    self.background_sfn.send_task_success(
        taskToken=self.task.task_token,
        output=output_json
      )
    self.cv.notify_all()

  def send_task_success(self, output_data: JsonableDict):
    with self.session_mutex:
      self.send_task_success_locked(output_data)

  def send_task_failure_locked(self, error: Any=None, cause: Jsonable=None):
    if error is None:
      error_str = "The activity task failed"
    else:
      error_str = str(error)
    if len(error_str) > 256:
      error_str = error_str[:256]
    if isinstance(cause, str):
      cause_str = cause
    else:
      cause_str = json.dumps(cause, sort_keys=True, separators=(',', ':'))
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    self.task_completed = True
    self.end_time_ns = time.monotonic_ns()
    print(f"Sending task_failure, error={error_str}, cause={json.dumps(cause, indent=2, sort_keys=True)}", file=sys.stderr)
    self.background_sfn.send_task_failure(
        taskToken=self.task.task_token,
        cause=cause_str,
        error=error_str
      )
    self.cv.notify_all()

  def send_task_failure(self, error: Any=None, cause: Jsonable=None):
    with self.session_mutex:
      self.send_task_failure_locked(error=error, cause=cause)

  def send_task_exception_locked(
        self,
        exc: BaseException,
        tb: Optional[TracebackType]=None,
        exc_type: Optional[Type[BaseException]]=None,
      ):
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
    with self.session_mutex:
      self.send_task_exception_locked(exc, tb=tb, exc_type=exc_type)

  def send_task_heartbeat_locked(self):
    if self.task_completed:
      raise RuntimeError("AWS stepfunctions task is already completed")
    print(f"Sending task_heartbeat", file=sys.stderr)
    self.background_sfn.send_task_heartbeat(
        taskToken=self.task.task_token
      )
    print(f"task_heartbeat sent successfully", file=sys.stderr)

  def send_task_heartbeat(self):
    with self.session_mutex:
      self.send_task_heartbeat_locked()

  def __enter__(self) -> 'AwsStepActivityTaskContext':
    print(f"Entering task context", file=sys.stderr)
    with self.session_mutex:
      if not self.background_thread is None:
        raise RuntimeError("AwsStepActivityHeartBeater.__enter__: Context already entered")
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

  def elapsed_time(self) -> Optional[float]:
    end_time_ns = self.end_time_ns
    if end_time_ns is None:
      end_time_ns = time.monotonic_ns()
    return (end_time_ns - self.start_time_ns) / 1000000000.0

  def remaining_time(self) -> Optional[float]:
    if self.max_total_seconds <= 0.0:
      return None
    return max(0.0, self.max_total_seconds - self.elapsed_time())

  def background_fn(self):
      print(f"Background thread starting", file=sys.stderr)
      with self.session_mutex:
        try:
          while True:
            if self.task_completed:
              break
            sleep_secs = self.heartbeat_seconds
            remaining_time_sec = self.remaining_time()
            if not remaining_time_sec is None:
              if remaining_time_sec <= 0.0:
                raise RuntimeError("Max task runtime exceeded")
              if remaining_time_sec < sleep_secs:
                sleep_secs = remaining_time_sec
            print(f"Background thread sleeping for {sleep_secs} seconds", file=sys.stderr)
            self.cv.wait(timeout=sleep_secs) # this will release the mutex while we sleep
            print(f"Background thread awake", file=sys.stderr)
            if self.task_completed:
              break
            remaining_time_sec = self.remaining_time()
            if not remaining_time_sec is None and remaining_time_sec <= 0.0:
                raise RuntimeError("Max task runtime exceeded")
            self.send_task_heartbeat_locked()
        except Exception as ex:
          print(f"Exception in background thread: {ex}", file=sys.stderr)
          try:
            self.send_task_exception(ex)
          except Exception:
            pass
      print(f"Background thread exiting", file=sys.stderr)
