# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""A client for invoking AWS step functions (
  i.e., creating and monitoring step function state machine
  executions) that wrap activities implemented by AwsStepActivityWorker"""
from .logging import logger

import sys
from time import sleep
from typing import TYPE_CHECKING, Optional, Dict, Type, Union, List, Tuple
from types import TracebackType

from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions
from mypy_boto3_stepfunctions.type_defs import LoggingConfigurationTypeDef, TracingConfigurationTypeDef
from .internal_types import Jsonable, JsonableDict


import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError
from .util import create_aws_session, full_type, normalize_jsonable_dict, normalize_jsonable_list
from .sfn_util import create_aws_step_activity, describe_aws_step_activity, describe_aws_step_state_machine

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

class AwsStepStateMachine:
  mutex: Lock
  cv: Condition
  session: Session
  sfn: SFNClient
  state_machine_desc: JsonableDict
  state_machine_name: str
  state_machine_arn: str
  _definition: JsonableDict
  dirty: bool = False

  def __init__(
        self,
        state_machine_id: str,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
      ):
    """Create a wrapper for an existing AWS Step Function State Machine

    Args:
        state_machine_id (str):
            The ARN or the name of the AWS stepfunction state machine.
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
    """

    self.mutex = Lock()
    self.cv = Condition(self.mutex)

    if session is None:
      session = Session(profile_name=aws_profile, region_name=aws_region)

    self.session = session

    sfn = self.session.client('stepfunctions')
    self.sfn = sfn

    desc = describe_aws_step_state_machine(sfn, state_machine_id)
    self._refresh_from_desc(desc)

  @property
  def comment(self) -> str:
    return self._definition['Comment']

  @comment.setter
  def comment(self, v: str) -> None:
    self._definition['Comment'] = v
    self.dirty = True

  @property
  def definition(self) -> JsonableDict:
    return self._definition

  @definition.setter
  def definition(self, v: JsonableDict) -> None:
    self._definition = normalize_jsonable_dict(v)
    self.dirty = True

  def update(
        self,
        definition: Optional[JsonableDict]=None,
        roleArn: Optional[str]=None,
        tracingEnabled: Optional[bool]=None,
        loggingLevel: Optional[str]=None,
        includeExecutionDataInLogs: Optional[bool]=None,
        loggingDestinations: Optional[List[JsonableDict]]=None
      ):
    definition_str = None if definition is None else json.dumps(definition, sort_keys=True, separators=(',', ':'))
    loggingConfiguration: LoggingConfigurationTypeDef = normalize_jsonable_dict(self.state_machine_desc['loggingConfiguration'])
    if roleArn is None:
      roleArn = self.state_machine_desc['roleArn']
    if not loggingLevel is None:
      loggingConfiguration['level'] = str(loggingLevel)
    if not includeExecutionDataInLogs is None:
      loggingConfiguration['includeExecutionData'] = not not includeExecutionDataInLogs
    if not loggingDestinations is None:
      loggingConfiguration['destinations'] = normalize_jsonable_list(loggingDestinations)
    tracingConfiguration: TracingConfigurationTypeDef = normalize_jsonable_dict(self.state_machine_desc['tracingConfiguration'])
    if not tracingEnabled is None:
      tracingConfiguration['enabled'] = tracingEnabled
    self.sfn.update_state_machine(
        stateMachineArn=self.state_machine_arn,
        definition=definition_str,
        roleArn=roleArn,
        loggingConfiguration=loggingConfiguration,
        tracingConfiguration=tracingConfiguration
      )
    self.refresh()

  def _refresh_from_desc(self, desc: JsonableDict):
    desc = normalize_jsonable_dict(desc)
    self.state_machine_desc = desc
    self.state_machine_arn = desc['stateMachineArn']
    self.state_machine_name = desc['name']
    self._definition = json.loads(desc['definition'])
    self.dirty = False

  def refresh(self):
    desc = describe_aws_step_state_machine(self.sfn, self.state_machine_arn)
    self._refresh_from_desc(desc)

  def flush(self, force: bool=False):
    if force or self.dirty:
      self.update(definition=self.definition)

  @property
  def states(self) -> Dict[str, JsonableDict]:
    return self.definition['States']

  def set_states(self, states: Dict[str, JsonableDict]):
    definition = normalize_jsonable_dict(self.definition)
    definition['States'] = normalize_jsonable_dict(states)
    self.definition = definition

  @property
  def num_states(self) -> int:
    return len(self.states)

  def get_state(self, name: str) -> JsonableDict:
    return self.states[name]

  def del_state(self, name: str):
    states = normalize_jsonable_dict(self.states)

  def set_state(self, name: str, state: JsonableDict):
    states = normalize_jsonable_dict(self.states)
    states[name] = normalize_jsonable_dict(state)
    self.set_states(states)

  def set_task_state(
        self,
        state_name: str,
        resource_arn: str,
        next_state: Optional[str]=None,
        parameters: Optional[JsonableDict]=None,
        result_path: Optional[str]=None,
        result_selector: Optional[JsonableDict]=None,
        retry: Optional[List[JsonableDict]]=None,
        catch: Optional[List[JsonableDict]]=None,
        timeout_seconds: Optional[int]=None,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[int]=None,
        heartbeat_seconds_path: Optional[str]=None,
      ):
    state: JsonableDict = dict(Type='Task', Resource=resource_arn)
    if next_state is None:
      state.update(End=True)
    else:
      state.update(Next=next_state)
    if not parameters is None:
      state.update(Parameters=parameters)
    if not result_path is None:
      state.update(ResultPath=result_path)
    if not result_selector is None:
      state.update(ResultPath=result_path)
    if not retry is None:
      state.update(Retry=retry)
    if not catch is None:
      state.update(Catch=catch)
    if not timeout_seconds is None:
      state.update(TimeoutSeconds=timeout_seconds)
    if not timeout_seconds_path is None:
      state.update(TimeoutSecondsPath=timeout_seconds_path)
    if not heartbeat_seconds is None:
      state.update(HeartbeatSeconds=heartbeat_seconds)
    if not heartbeat_seconds_path is None:
      state.update(HeartbeatSecondsPath=heartbeat_seconds_path)
    self.set_state(state_name, state)

  def set_activity_state(
        self,
        state_name: str,
        activity_id: str,
        create_activity: bool=True,
        allow_activity_exists: bool=True,
        next_state: Optional[str]=None,
        parameters: Optional[JsonableDict]=None,
        result_path: Optional[str]=None,
        result_selector: Optional[JsonableDict]=None,
        retry: Optional[List[JsonableDict]]=None,
        catch: Optional[List[JsonableDict]]=None,
        timeout_seconds: Optional[int]=None,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[int]=None,
        heartbeat_seconds_path: Optional[str]=None,
      ):
    if create_activity:
      activity_desc = self.create_activity(activity_id, allow_exists=allow_activity_exists)
    else:
      activity_desc = self.describe_activity(activity_id)
    resource_arn = activity_desc['activityArn']
    self.set_task_state(
        state_name,
        resource_arn,
        next_state=next_state,
        parameters=parameters,
        result_path=result_path,
        result_selector=result_selector,
        retry=retry,
        catch=catch,
        timeout_seconds=timeout_seconds,
        timeout_seconds_path=timeout_seconds_path,
        heartbeat_seconds=heartbeat_seconds,
        heartbeat_seconds_path=heartbeat_seconds_path)

  def describe_activity(
        self,
        activity_id: str
      ) -> JsonableDict:
    return describe_aws_step_activity(self.sfn, activity_id)

  def create_activity(
        self,
        activity_id: str,
        allow_exists: bool=False
      ) -> JsonableDict:
    if allow_exists:
      try:
        result = self.describe_activity(activity_id)
        return result
      except RuntimeError as ex:
        if not str(ex).endswith('was not found'):
          raise
    result = create_aws_step_activity(self.sfn, activity_id)
    return result

  def is_choice_state(self, state_name: str) -> bool:
    state = self.get_state(state_name)
    return state['Type'] == 'Choice'

  def is_param_choice_state(self, state_name: str) -> bool:
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      return False
    var_name: Optional[str] = None
    choices = state['Choices']
    for i, choice in enumerate(choices):
      if not 'Variable' in choice or not 'Next' in choice:
        return False
      vn = choice['Variable']
      if not vn.startswith('$.'):
        return False
      vn = vn[2:]
      if var_name is None:
        var_name = vn
      elif vn != var_name:
        return False
      if i == 0 and 'IsPresent' in choice and not choice['IsPresent']:
        pass
      elif 'StringEquals' in choice:
        pass
      else:
        return False
    return True

  def get_param_choice_next_states(self, state_name: str) -> Tuple[Optional[str], Dict[Optional[str], str]]:
    """For a named parameter value selection Choice state, return the selection parameter name and a map of values to next state names.

    Args:
        state_name (str): The name of the state, which must be a Choice state that selects from values of a named parameter

    Raises:
        RuntimeError: The named state does not exist
        RuntimeError: The named state is not a parameter value selection Choice state

    Returns:
        Tuple[Optional[str], Dict[Optional[str], str]]:
            A Tuple consisting of:
              [0]: The name of the parameter that is being used to select the next state. None only
                   if it cannot be determined because the choice list is empty.
              [1]: A dictionary that maps parameter values to the next state name. A key of None
                   is representative of a default choice to be used if the specified parameter
                   is not present in the state inputs.
    """
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      raise RuntimeError(f'State "{state_name}" is not a Choice state')
    param_name: Optional[str] = None
    choices: List[JsonableDict] = state['Choices']
    result: Dict[Optional[str], str] = {}
    for i, choice in enumerate(choices):
      choice_value: Optional[str]
      if not 'Variable' in choice or not 'Next' in choice:
        raise RuntimeError(f'Choice #{i} in state "{state_name}" does not have a Variable field')
      next_state = choice['Next']
      vn = choice['Variable']
      if not vn.startswith('$.'):
        raise RuntimeError(f'Choice #{i} in state "{state_name}" Variable field does not begin with "$."')
      vn = vn[2:]
      if param_name is None:
        param_name = vn
      elif vn != param_name:
        raise RuntimeError(f'Choice #{i} in state "{state_name}" has inconsistent Variable name "{vn}" (prior="{param_name}"')
      if i == 0 and 'IsPresent' in choice and not choice['IsPresent']:
        choice_value = None
      elif 'StringEquals' in choice:
        choice_value = choice['StringEquals']
        if choice_value in result:
          raise RuntimeError(f'Choice #{i} in state "{state_name}" has duplicate StringEquals choice value "{choice_value}"')
      else:
        raise RuntimeError(f'Choice #{i} in state "{state_name}" does not have a StringEquals field (or an IsPresent=false field as the first choice)')
      result[choice_value] = next_state
    return param_name, result

  def get_param_choice_next_state(self, state_name: str, choice_value:Optional[str], param_name:Optional[str]=None) -> str:
    actual_param_name, next_states = self.get_param_choice_next_states(state_name)
    if not param_name is None and not actual_param_name is None and param_name != actual_param_name:
        raise RuntimeError(f'Choice state "{state_name}" actual parameter name "{actual_param_name}" does not match expected parameter name "{param_name}"')
    if not choice_value in next_states:
        raise RuntimeError(f'Choice value {json.dumps(choice_value)} is not present in choice state "{state_name}"')
    return next_states[choice_value]

  def set_param_choice_next_states(self, state_name: str, param_name: str, next_states: Dict[Optional[str], str]):
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      raise RuntimeError(f'State "{state_name}" is not a Choice state')
    choices: List[JsonableDict] = []
    vn = f"$.{param_name}"
    if None in next_states:
      choices.append(dict(Variable=vn, IsPresent=False, Next=next_states[None]))
    choice_value: Optional[str]
    for choice_value in sorted(x for x in next_states.keys() if not x is None):
      choices.append(dict(Variable=vn, StringEquals=choice_value, Next=next_states[choice_value]))
    self.set_state_choices(state_name, choices)

  def set_param_choice_next_state(self, state_name: str, choice_value:Optional[str], next_state: str, param_name:Optional[str]=None):
    actual_param_name, next_states = self.get_param_choice_next_states(state_name)
    if param_name is None:
      if actual_param_name is None:
        raise RuntimeError(f'Choice state "{state_name}" param_name must be provided for first added choice"')
      param_name = actual_param_name
    else:
      if not not actual_param_name is None and param_name != actual_param_name:
        raise RuntimeError(f'Choice state "{state_name}" actual parameter name "{actual_param_name}" does not match expected parameter name "{param_name}"')
    next_states[choice_value] = next_state
    self.set_param_choice_next_states(state_name, param_name, next_states)

  def del_param_choice(self, state_name: str, choice_value:Optional[str], param_name:Optional[str]=None, must_exist: bool=False) -> None:
    actual_param_name, next_states = self.get_param_choice_next_states(state_name)
    if param_name is None:
      if actual_param_name is None:
        raise RuntimeError(f'Choice state "{state_name}" param_name must be provided for first added choice"')
      param_name = actual_param_name
    else:
      if not not actual_param_name is None and param_name != actual_param_name:
        raise RuntimeError(f'Choice state "{state_name}" actual parameter name "{actual_param_name}" does not match expected parameter name "{param_name}"')
    if choice_value in next_states:
      del next_states[choice_value]
      self.set_param_choice_next_states(state_name, param_name, next_states)
    elif must_exist:
      raise KeyError(f'Choice state "{state_name}", choice value "{choice_value}" does not exist')

  def get_state_choices(self, state_name: str) -> List[JsonableDict]:
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      raise RuntimeError(f'State "{state_name}" is not of Type "Choice"')
    choices = state['Choices']

  def set_state_choices(self, state_name: str, choices: List[JsonableDict]):
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      raise RuntimeError(f'State "{state_name}" is not of Type "Choice"')
    state['Choices'] = normalize_jsonable_list(choices)
    self.set_state(state_name, state)

  def get_state_num_choices(self, state_name: str) -> int:
    return len(self.get_state_choices(state_name))

  def del_state_choice(self, state_name: str, index: int):
    choices = self.get_state_choices(state_name)
    choices.pop(index)
    self.set_state_choices(state_name, choices)

  def insert_state_choice(self, state_name: str, choice: JsonableDict, index: int=-1):
    choices = self.get_state_choices(state_name)
    if index < 0:
      index = len(choices) + index + 1
    choices.insert(index, normalize_jsonable_dict(choice))
    self.set_state_choices(state_name, choices)

  def is_param_value_choice(self, choice: JsonableDict):
    return 'Variable' in choice and choice['Variable'].startswith('$.') and 'StringEquals' in choice and 'Next' in choice

  def choice_param_name(self, choice: JsonableDict) -> str:
    var_name = choice['Variable']
    if not var_name.startswith('$.'):
      raise RuntimeError(f"Choice definition Variable name does not begin with '$.': '{var_name}'")
    return var_name[2:]

  def choice_next_state(self, choice: JsonableDict) -> str:
    state_name = choice['Next']
    return state_name

  def is_param_default_choice(self, choice: JsonableDict):
    return 'Variable' in choice and choice['Variable'].startswith('$.') and 'IsPresent' in choice and not choice['IsPresent'] and 'Next' in choice
