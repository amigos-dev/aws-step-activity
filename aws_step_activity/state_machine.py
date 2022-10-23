# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""A client for invoking AWS step functions (
  i.e., creating and monitoring step function state machine
  executions) that wrap activities implemented by AwsStepActivityWorker"""
from .logging import logger

import sys
from time import monotonic_ns, sleep
from typing import TYPE_CHECKING, Optional, Dict, Type, Union, List, Tuple, Set, Any, Generator, IO
from types import TracebackType

from .internal_types import (
    Jsonable,
    JsonableDict,
    SFNClient,
    SFN_LoggingConfigurationTypeDef as LoggingConfigurationTypeDef,
    SFN_TracingConfigurationTypeDef as TraingConfigurationTypeDef,
  )


import boto3
from boto3 import Session
from botocore.exceptions import ReadTimeoutError

from .util import (
    create_aws_session,
    full_type,
    normalize_jsonable_dict,
    normalize_jsonable_list,
    get_aws_account
  )

from .sfn_util import (
    create_aws_step_activity,
    describe_aws_step_activity,
    delete_aws_step_activity,
    describe_aws_step_state_machine,
    describe_aws_step_execution,
    is_aws_step_activity_arn,
    get_aws_step_activity_name_from_arn,
    create_aws_step_state_machine,
    get_aws_step_state_machine_and_execution_names_from_arn,
  )

from .s3_util import S3Client, s3_download_object_to_fileobj

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
  s3: S3Client
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
    s3 = self.session.client('s3')
    self.s3 = s3

    desc = describe_aws_step_state_machine(sfn, state_machine_id)
    self._refresh_from_desc(desc)

  @classmethod
  def create(
        cls,
        state_machine_id: str,
        states: Optional[Dict[str, JsonableDict]]=None,
        start_at: str= 'Start',
        comment: Optional[str]=None,
        state_machine_type: str='STANDARD',
        timeout_seconds: Optional[Union[int, float]]=None,
        tracingEnabled: bool=True,
        loggingLevel: Optional[str]=None,
        includeExecutionDataInLogs: Optional[bool]=None,
        loggingDestinations: Optional[List[JsonableDict]]=None,
        add_default_cloudwatch_log_destination: bool=True,
        role_id: Optional[str]=None,
        role_path: str='/service-role/',
        assume_role_policy_document: Optional[Union[str, JsonableDict]]=None,
        role_description: Optional[str]=None,
        role_max_session_duration: int=3600,
        role_permissions_boundary: Optional[JsonableDict]=None,
        role_add_policies: Optional[Dict[str, Optional[Union[str, JsonableDict]]]]=None,
        role_add_cloudwatch_policy: bool=True,
        role_add_xray_policy: bool=True,
        allow_role_exists: bool=True,
        allow_exists: bool=False,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
      ) -> 'AwsStepStateMachine':
    if not timeout_seconds is None and timeout_seconds == 0.0:
      timeout_seconds = None
    if loggingLevel is None:
      loggingLevel='ALL'
    if session is None:
      session = Session(profile_name=aws_profile, region_name=aws_region)
    desc = create_aws_step_state_machine(
        state_machine_id=state_machine_id,
        states=states,
        session=session,
        start_at=start_at,
        comment=comment,
        state_machine_type=state_machine_type,
        timeout_seconds=timeout_seconds,
        tracingEnabled=tracingEnabled,
        loggingLevel=loggingLevel,
        includeExecutionDataInLogs=includeExecutionDataInLogs,
        loggingDestinations=loggingDestinations,
        add_default_cloudwatch_log_destination=add_default_cloudwatch_log_destination,
        role_id=role_id,
        role_path=role_path,
        assume_role_policy_document=assume_role_policy_document,
        role_description=role_description,
        role_max_session_duration=role_max_session_duration,
        role_permissions_boundary=role_permissions_boundary,
        role_add_policies=role_add_policies,
        role_add_cloudwatch_policy=role_add_cloudwatch_policy,
        role_add_xray_policy=role_add_xray_policy,
        allow_role_exists=allow_role_exists,
        allow_exists=allow_exists,
      )
    result = AwsStepStateMachine(desc['stateMachineArn'], session=session)
    return result

  @classmethod
  def create_with_activity_choices(
        cls,
        state_machine_id: str,
        activity_ids: Optional[List[str]]=None,
        default_activity_id: Optional[str]=None,
        comment: Optional[str]=None,
        state_machine_type: str='STANDARD',
        tracingEnabled: bool=True,
        loggingLevel: Optional[str]=None,
        includeExecutionDataInLogs: Optional[bool]=None,
        loggingDestinations: Optional[List[JsonableDict]]=None,
        add_default_cloudwatch_log_destination: bool=True,
        role_id: Optional[str]=None,
        role_path: str='/service-role/',
        assume_role_policy_document: Optional[Union[str, JsonableDict]]=None,
        role_description: Optional[str]=None,
        role_max_session_duration: int=3600,
        role_permissions_boundary: Optional[JsonableDict]=None,
        role_add_policies: Optional[Dict[str, Optional[Union[str, JsonableDict]]]]=None,
        role_add_cloudwatch_policy: bool=True,
        role_add_xray_policy: bool=True,
        allow_role_exists: bool=True,
        allow_exists: bool=False,
        timeout_seconds: Optional[Union[int, float]]=None,
        activity_heartbeat_seconds: Optional[Union[int, float]]=None,
        activity_timeout_seconds: Optional[Union[int, float]]=None,
        session: Optional[Session]=None,
        aws_profile: Optional[str]=None,
        aws_region: Optional[str]=None,
      ) -> 'AwsStepStateMachine':
    if not timeout_seconds is None and timeout_seconds == 0.0:
      timeout_seconds = None
    if not activity_timeout_seconds is None and activity_timeout_seconds == 0.0:
      activity_timeout_seconds = None
    if not activity_heartbeat_seconds is None and activity_heartbeat_seconds == 0.0:
      activity_heartbeat_seconds = None
    
    variable = '$.activity'
    if session is None:
      session = Session(profile_name=aws_profile, region_name=aws_region)
    sfn = session.client('stepfunctions')
    choices: List[JsonableDict] = []
    if activity_ids is None:
      activity_ids = []
    activity_map: Dict[str, str] = {}  # map from activity name to activity ARN
    default_activity_name: Optional[str] = None
    if not default_activity_id is None:
      default_activity_info = create_aws_step_activity(sfn, default_activity_id, allow_exists=True)
      default_activity_arn = default_activity_info['activityArn']
      default_activity_name = default_activity_info['name']
      activity_map[default_activity_name] = default_activity_arn
    for activity_id in activity_ids:
      activity_info = create_aws_step_activity(sfn, activity_id, allow_exists=True)
      activity_arn = activity_info['activityArn']
      activity_map[activity_info['name']] = activity_arn

    choices: List[JsonableDict] = []
    if not default_activity_name is None:
      choices.append(dict(IsPresent=False, Next=f'Run-{default_activity_name}', Variable=variable))
    for activity_name in sorted(activity_map.keys()):
      choices.append(dict(StringEquals=activity_name, Next=f'Run-{activity_name}', Variable=variable))

    states = dict(
        Start=dict(Type='Pass', Next='SelectActivity'),
        SelectActivity=dict(Type='Choice', Choices=choices),
        Final=dict(Type='Pass', End=True)
      )
    for activity_name, activity_arn in activity_map.items():
      state = dict(Type='Task', Next='Final', Resource=activity_arn)
      if not activity_heartbeat_seconds is None:
        state['HeartbeatSeconds'] = round(activity_heartbeat_seconds)
      if not activity_timeout_seconds is None:
        state['TimeoutSeconds'] = round(activity_timeout_seconds)
      states[f'Run-{activity_name}'] = state
    result = cls.create(
          state_machine_id=state_machine_id,
          states=states,
          session=session,
          start_at='Start',
          comment=comment,
          state_machine_type=state_machine_type,
          timeout_seconds=timeout_seconds,
          tracingEnabled=tracingEnabled,
          loggingLevel=loggingLevel,
          includeExecutionDataInLogs=includeExecutionDataInLogs,
          loggingDestinations=loggingDestinations,
          add_default_cloudwatch_log_destination=add_default_cloudwatch_log_destination,
          role_id=role_id,
          role_path=role_path,
          assume_role_policy_document=assume_role_policy_document,
          role_description=role_description,
          role_max_session_duration=role_max_session_duration,
          role_permissions_boundary=role_permissions_boundary,
          role_add_policies=role_add_policies,
          role_add_cloudwatch_policy=role_add_cloudwatch_policy,
          role_add_xray_policy=role_add_xray_policy,
          allow_role_exists=allow_role_exists,
          allow_exists=allow_exists,
        )
    return result

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
    logger.debug(f'Updating state machine {self.state_machine_name}, roleArn="{roleArn}, definition={json.dumps(definition, sort_keys=True, indent=2)}"')
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
    if name in states:
      del states[name]
    self.set_states(states)

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
        timeout_seconds: Optional[Union[int, float]]=None,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[Union[int, float]]=None,
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
      state.update(TimeoutSeconds=round(timeout_seconds))
    if not timeout_seconds_path is None:
      state.update(TimeoutSecondsPath=timeout_seconds_path)
    if not heartbeat_seconds is None:
      state.update(HeartbeatSeconds=round(heartbeat_seconds))
    if not heartbeat_seconds_path is None:
      state.update(HeartbeatSecondsPath=heartbeat_seconds_path)
    self.set_state(state_name, state)

  def set_activity_state(
        self,
        activity_id: str,
        state_name: Optional[str]=None,
        create_activity: bool=True,
        allow_activity_exists: bool=True,
        next_state: Optional[str]='Final',
        parameters: Optional[JsonableDict]=None,
        result_path: Optional[str]=None,
        result_selector: Optional[JsonableDict]=None,
        retry: Optional[List[JsonableDict]]=None,
        catch: Optional[List[JsonableDict]]=None,
        timeout_seconds: Optional[Union[int, float]]=600,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[Union[int, float]]=60,
        heartbeat_seconds_path: Optional[str]=None,
      ):
    if create_activity:
      activity_desc = self.create_activity(activity_id, allow_exists=allow_activity_exists)
    else:
      activity_desc = self.describe_activity(activity_id)
    activity_name = activity_desc['name']
    if state_name is None:
      state_name = f'Run-{activity_name}'
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

  def del_activity_state(
        self,
        state_name: str,
        delete_activity: bool=False
      ):
    state = self.get_state(state_name)
    state_type = state['Type']
    if state_type != 'Task':
      raise RuntimeError(f'State "{state_name}" Type is not "Task": "{state_type}"')
    activity_arn = state['Resource']
    if not is_aws_step_activity_arn(activity_arn):
      raise RuntimeError(f'State "{state_name}" task Resource is not an activity ARN: "{activity_arn}"')
    self.del_state(state_name)
    if delete_activity:
      self.delete_activity(activity_arn, must_exist=False)

  def describe_activity(
        self,
        activity_id: str
      ) -> JsonableDict:
    return describe_aws_step_activity(self.sfn, activity_id)

  def create_activity(
        self,
        activity_id: str,
        allow_exists: bool=True,
      ) -> JsonableDict:
    result = create_aws_step_activity(self.sfn, activity_id, allow_exists=allow_exists)
    return result

  def delete_activity(
        self,
        activity_id: str,
        must_exist: bool=False
      ) -> None:
    delete_aws_step_activity(self.sfn, activity_id, must_exist=must_exist)

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

  def set_param_choice_next_states(self, state_name: str, param_name: Optional[str], next_states: Dict[Optional[str], str]):
    old_next_states: Set[str] = set()
    new_next_states = set(next_states.values())
    state = self.get_state(state_name)
    if state['Type'] != 'Choice':
      raise RuntimeError(f'State "{state_name}" is not a Choice state')
    for old_choice in state['Choices']:
      if param_name is None and 'Variable' in old_choice:
        vn: str = old_choice['Variable']
        if vn.startswith('$.'):
          param_name = vn[2:]
      old_next_states.add(old_choice['Next'])
    choices: List[JsonableDict] = []
    if param_name is None:
      raise RuntimeError("param_name cannot be inferred from previous Choice state")
    vn = f"$.{param_name}"
    if None in next_states:
      choices.append(dict(Variable=vn, IsPresent=False, Next=next_states[None]))
    choice_value: Optional[str]
    for choice_value in sorted(x for x in next_states.keys() if not x is None):
      choices.append(dict(Variable=vn, StringEquals=choice_value, Next=next_states[choice_value]))
    self.set_state_choices(state_name, choices)
    # delete all the states that are no longer referenced by the choices
    deleted_states = old_next_states.difference(new_next_states)
    if len(deleted_states) > 0:
      logger.debug(f'Choice State "{state_name}": next states {list(deleted_states)} are no longer referenced and will be deleted')
    for deleted_state in deleted_states:
      self.del_state(deleted_state)

  def get_activity_choices(
        self,
        state_name: str="SelectActivity",
      ) -> Tuple[List[str], Optional[str]]:
    """Returns the list of activity names selectable by an activity choice state, and the default choice (if any).

    Args:
        state_name (str, optional): The activity choice state name. Defaults to "SelectActivity".

    Raises:
        RuntimeError: The state is not a valid activity choice state

    Returns:
        Tuple[List[str], Optional[str]]: A Tuple consisting of:
           [0]: A list of activity names that may be selected explicitly by this Choice state
           [1]: The default activity name that is selected by this Choice state. If None, there is no default.
    """
    param_name, choice_map = self.get_param_choice_next_states(state_name)
    default_activity_name: Optional[str] = None
    activity_names: Set[str] = set()
    for activity_name, next_state in choice_map.items():
      next_state_info = self.get_state(next_state)
      if next_state_info['Type'] != 'Task':
        raise RuntimeError(f'State "{state_name}" next state "{next_state}" is not a Task state')
      next_activity_arn = next_state_info['Resource']
      if not is_aws_step_activity_arn(next_activity_arn):
        raise RuntimeError(f'State "{state_name}" next state "{next_state}" is not an activity Task state')
      actual_activity_name = get_aws_step_activity_name_from_arn(next_activity_arn)
      if not activity_name is None and activity_name != actual_activity_name:
        raise RuntimeError(f'State "{state_name}" choice "{activity_name}" does not match activity name in target state "{actual_activity_name}"')
      activity_names.add(actual_activity_name)
      if activity_name is None:
        default_activity_name = actual_activity_name
    return sorted(activity_names), default_activity_name

  def set_activity_choices(
        self,
        state_name: str="SelectActivity",
        param_name: Optional[str]="activity",
        activity_ids: Optional[List[str]]=None,
        default_activity_id: Optional[str]=None,
        activity_next_state: Optional[str]='::FinalOrNone::',
        timeout_seconds: Optional[Union[int, float]]=600,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[Union[int, float]]=60,
        heartbeat_seconds_path: Optional[str]=None,
      ):
    if activity_next_state == '::FinalOrNone::':
      activity_next_state = 'Final' if 'Final' in self.states else None
    if activity_ids is None:
      activity_ids = []
    activity_map: Dict[str, str] = {}  # map from activity name to activity ARN
    default_activity_name: Optional[str] = None
    if not default_activity_id is None:
      default_activity_info = create_aws_step_activity(self.sfn, default_activity_id, allow_exists=True)
      default_activity_arn = default_activity_info['activityArn']
      default_activity_name = default_activity_info['name']
      activity_map[default_activity_name] = default_activity_arn
    for activity_id in activity_ids:
      activity_info = create_aws_step_activity(self.sfn, activity_id, allow_exists=True)
      activity_arn = activity_info['activityArn']
      activity_map[activity_info['name']] = activity_arn
    next_states: Dict[Optional[str], str] = {}
    if not default_activity_name is None:
      next_states[None] = f'Run-{default_activity_name}'
    for activity_name in sorted(activity_map.keys()):
      next_states[activity_name] = f'Run-{activity_name}'
    for choice_next_state in next_states.values():
      if not choice_next_state in self.states:
        activity_name = choice_next_state[4:]
        self.set_activity_state(
            activity_name,
            choice_next_state,
            next_state=activity_next_state,
            timeout_seconds=timeout_seconds,
            timeout_seconds_path=timeout_seconds_path,
            heartbeat_seconds=heartbeat_seconds,
            heartbeat_seconds_path=heartbeat_seconds_path
          )
    self.set_param_choice_next_states(state_name, param_name, next_states)

  def add_activity_choice(
        self,
        activity_id: str,
        state_name: str="SelectActivity",
        param_name: Optional[str]="activity",
        is_default: bool=False,
        activity_next_state: Optional[str]='::FinalOrNone::',
        timeout_seconds: Optional[Union[int, float]]=600,
        timeout_seconds_path: Optional[str]=None,
        heartbeat_seconds: Optional[Union[int, float]]=60,
        heartbeat_seconds_path: Optional[str]=None,
      ):
    activity_name_list, default_activity = self.get_activity_choices(state_name)
    activity_info = create_aws_step_activity(self.sfn, activity_id, allow_exists=True)
    activity_name = activity_info['name']
    activity_arn = activity_info['activityArn']
    activity_names = set(activity_name_list)
    activity_names.add(activity_name)
    if is_default:
      default_activity = activity_name
    else:
      if default_activity == activity_name:
        default_activity = None
    self.set_activity_choices(
        state_name=state_name,
        param_name=param_name,
        activity_ids=sorted(activity_names),
        default_activity_id=default_activity,
        activity_next_state=activity_next_state,
        timeout_seconds=timeout_seconds,
        timeout_seconds_path=timeout_seconds_path,
        heartbeat_seconds=heartbeat_seconds,
        heartbeat_seconds_path=heartbeat_seconds_path
      )

  def del_activity_choice(
        self,
        activity_id: str,
        state_name: str="SelectActivity",
        param_name: Optional[str]="activity",
        activity_next_state: Optional[str]='::FinalOrNone::'
      ):
    activity_name = get_aws_step_activity_name_from_arn(activity_id) if ':' in activity_id else activity_id
    activity_name_list, default_activity = self.get_activity_choices(state_name)
    activity_names = set(activity_name_list)
    if default_activity == activity_name:
      default_activity = None
    activity_names.discard(activity_name)
    self.set_activity_choices(
        state_name=state_name,
        param_name=param_name,
        activity_ids=sorted(activity_names),
        default_activity_id=default_activity,
        activity_next_state=activity_next_state
      )

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

  @classmethod  
  def gen_execution_name(cls) -> str:
    result = datetime.utcnow().isoformat()[:19].replace(':', '-') +'Z-' + str(uuid.uuid4())
    return result

  def start_execution(
        self,
        name: Optional[str]=None,
        input_data: Optional[Union[str, JsonableDict]]=None,
        trace_header: Optional[str]=None,
      ) -> JsonableDict:
    if name is None:
      name = self.gen_execution_name()
    input_data_str: str
    if input_data is None:
      input_data = {}
      input_data_str = '{}'
    elif isinstance(input_data, str):
      input_data = json.loads(input_data)
      input_data_str = input_data
    else:
      input_data_str = json.dumps(input_data, sort_keys=True, separators=(',', ':'))
    params = dict(stateMachineArn=self.state_machine_arn, name=name, input=input_data_str)
    if trace_header != None:
      params.update(traceHeader=trace_header)
    resp = self.sfn.start_execution(**params)
    result = normalize_jsonable_dict(resp)
    result['name'] = name
    result['state_machine_arn'] = self.state_machine_arn
    result['state_machine_name'] = self.state_machine_name
    result['input'] = input_data
    if not trace_header is None:
      result['trace_header'] = trace_header

    return result

  def describe_execution(
        self,
        execution_id: str,
      ) -> JsonableDict:
    resp = describe_aws_step_execution(
        self.sfn,
        execution_id,
        state_machine_id=self.state_machine_arn
      )
    result = normalize_jsonable_dict(resp)
    state_machine_name, execution_name = get_aws_step_state_machine_and_execution_names_from_arn(result['executionArn'])
    result['state_machine_name'] = state_machine_name
    result['execution_name'] = execution_name
    return result

  def wait_for_execution(
        self,
        execution_id: str,
        polling_interval_seconds: Union[float, int]=10,
        max_wait_seconds: Optional[Union[float, int]]=None
      ) -> JsonableDict:
    start_time_ns = monotonic_ns()
    polling_interval_ns = round(polling_interval_seconds * 1000000000.0)
    max_wait_ns: Optional[int] = None if max_wait_seconds is None else round(max_wait_seconds * 1000000000.0)
    while True:
      result = self.describe_execution(execution_id)
      if result['status'] != 'RUNNING':
        return result
      sleep_ns = polling_interval_ns
      if not max_wait_ns is None:
        elapsed_ns = monotonic_ns() - start_time_ns
        remaining_ns = max_wait_ns - elapsed_ns
        if remaining_ns <= 0:
          raise TimeoutError("Timed out waiting for execution to complete")
        sleep_ns = min(sleep_ns, remaining_ns)
      sleep(sleep_ns/1000000000.0)

  def download_execution_output_file_to_fileobj(
        self,
        execution_id: str,
        output_filename: str,
        f: IO,
      ):
    exec_result = self.describe_execution(execution_id)
    input_data_str: Optional[str] = exec_result.get('input', None)
    if input_data_str is None:
      raise RuntimeError(f'Execution metadata does not include input data: {execution_id}')
    input_data: JsonableDict = json.loads(input_data_str)
    s3_outputs: Optional[str] = input_data.get('s3_outputs', None)
    if s3_outputs is None:
      raise RuntimeError(f'Execution does not provide s3_outputs: {execution_id}')
    while output_filename.startswith('/'):
      output_filename = output_filename[1:]
    while s3_outputs.endswith('/'):
      s3_outputs = s3_outputs[:-1]
    s3_url = s3_outputs + '/' + output_filename
    s3_download_object_to_fileobj(s3_url, f, s3=self.s3, session=self.session)

  def list_some_executions(
        self,
        max_results: int=1000,
        next_token: Optional[str]=None,
        status_filter: Optional[str]=None
      ) -> JsonableDict:
    params: Dict[str, Any] = dict(stateMachineArn=self.state_machine_arn, maxResults=max_results)
    if not next_token is None:
      params['nextToken'] = next_token
    if not status_filter is None:
      params['statusFilter'] = status_filter
    resp = self.sfn.list_executions(**params)
    result = normalize_jsonable_dict(resp)
    return result

  def iter_executions(
        self,
        next_token: Optional[str]=None,
        status_filter: Optional[str]=None
      ) -> Generator[JsonableDict, None, None]:
    params: Dict[str, Any] = dict(stateMachineArn=self.state_machine_arn, maxResults=max_results)
    if not status_filter is None:
      params['statusFilter'] = status_filter
    paginator = self.sfn.get_paginator('list_executions')
    page_iterator = paginator.paginate(**params)
    for page in page_iterator:
      for execution_desc in page['executions']:
        yield normalize_jsonable_dict(execution_desc)
