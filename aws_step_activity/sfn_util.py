# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""General utility functions for working with AWS step functions"""

import uuid
from .logging import logger

from typing import TYPE_CHECKING, Optional, Type, Any, Dict, Tuple, Generator, IO, List, Union
from .internal_types import (
    Jsonable,
    JsonableDict,
    SFNClient,
    IAMClient,
    CloudWatchLogsClient,
    SFN_LoggingConfigurationTypeDef as LoggingConfigurationTypeDef,
    SFN_TracingConfigurationTypeDef as TracingConfigurationTypeDef,
  )

import os
import sys
import re
import json
import boto3
import botocore
import botocore.session
import time
from boto3 import Session
from botocore.exceptions import ClientError

from urllib.parse import urlparse
import urllib.parse

from .util import (
    create_aws_session,
    full_type,
    normalize_jsonable_dict,
    normalize_jsonable_list,
    get_aws_account,
  )

def describe_aws_step_state_machine(
      sfn: SFNClient,
      state_machine_id: str
    ) -> JsonableDict:
  """Returns the result of SFNClient.describe_state_machine() for the given state machine name or ARN.

  Args:
      sfn (SFNClient): The boto3 client for AWS step functions
      state_machine_id (str): Either an AWS step function state machine name, or its ARN

  Raises:
      RuntimeError: The specified state_machine_id does not exist.

  Returns:
      JsonableDict: a dict as returned by SFNClient.describe_state_machine; e.g.:L

          {
              "stateMachineArn": "arn:aws:states:us-west-2:745019234935:stateMachine:RunSelectedActivity",
              "name": "RunSelectedActivity",
              "status": "ACTIVE",
              "definition": "{\n  \"Comment\": \"Run a simple AWS stepfunction activity task\",\n  \"StartAt\": \"SelectActivity\",\n  \"States\": {\n    \"SelectActivity\": {\n      \"Type\": \"Choice\",\n      \"Choices\": [\n        {\n          \"Variable\": \"$.activity\",\n          \"IsPresent\": false,\n          \"Next\": \"Run-Sam-MSI-worker\"\n        },\n        {\n          \"Variable\": \"$.activity\",\n          \"StringEquals\": \"Sam-MSI-worker\",\n          \"Next\": \"Run-Sam-MSI-worker\"\n        },\n        {\n          \"Variable\": \"$.activity\",\n          \"StringEquals\": \"Sam-linux-cpu-worker\",\n          \"Next\": \"NoOpTest\"\n        }\n      ]\n    },\n    \"NoOpTest\": {\n      \"Type\": \"Pass\",\n      \"End\": true\n    },\n    \"Run-Sam-MSI-worker\": {\n      \"Type\": \"Task\",\n      \"Resource\": \"arn:aws:states:us-west-2:745019234935:activity:Sam-MSI-worker\",\n      \"End\": true,\n      \"HeartbeatSeconds\": 40,\n      \"TimeoutSeconds\": 600\n    }\n  }\n}",
              "roleArn": "arn:aws:iam::745019234935:role/service-role/StepFunctions-RunSelectedActivity-role-7829293e",
              "type": "STANDARD",
              "creationDate": "2022-10-02T12:20:01.690000-07:00",
              "loggingConfiguration": {
                  "level": "ALL",
                  "includeExecutionData": true,
                  "destinations": [
                      {
                          "cloudWatchLogsLogGroup": {
                              "logGroupArn": "arn:aws:logs:us-west-2:745019234935:log-group:/aws/vendedlogs/states/RunSelectedActivity-Logs:*"
                          }
                      }
                  ]
              },
              "tracingConfiguration": {
                  "enabled": true
              }
          } 
  """
  state_machine_arn: str
  result: JsonableDict
  if ':' in state_machine_id:
    # state_machine_id must be an ARN.
    state_machine_arn = state_machine_id
  else:
    # state_machine_id must be a state machine name. Enumerate all state machines and
    # find the matching name
    state_machine_name_to_arn: Dict[str, str] = {}
    paginator = sfn.get_paginator('list_state_machines')
    page_iterator = paginator.paginate()
    for page in page_iterator:
      for state_machine_desc in page['stateMachines']:
        state_machine_name_to_arn[state_machine_desc['name']] = state_machine_desc['stateMachineArn']
    if not state_machine_id in state_machine_name_to_arn:
      raise RuntimeError(f"AWS stepfunctions state machine name '{state_machine_id}' was not found")
    state_machine_arn = state_machine_name_to_arn[state_machine_id]

  resp = sfn.describe_state_machine(stateMachineArn=state_machine_arn)
  result = normalize_jsonable_dict(resp)

  return result

DEFAULT_AWS_STEP_ASSUME_ROLE_POLICY_DOCUMENT: JsonableDict = {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "states.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }

DEFAULT_CLOUDWATCH_LOGS_DELIVERY_POLICY: JsonableDict = {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ],
        "Resource": "*"
      }
    ]
  }

DEFAULT_XRAY_ACCESS_POLICY: JsonableDict = {
    "Version": "2012-10-17",
    "Statement": [
        {
          "Effect": "Allow",
          "Action": [
              "xray:PutTraceSegments",
              "xray:PutTelemetryRecords",
              "xray:GetSamplingRules",
              "xray:GetSamplingTargets"
            ],
          "Resource": [
              "*"
            ]
        }
      ]
  }

def get_aws_policy_arn(
      iam: IAMClient,
      policy_id: str,
    ) -> str:
  policy_arn: str
  if not ':' in policy_id:
    policy_name_to_arn: Dict[str, str] = {}
    paginator = iam.get_paginator('list_policies')
    page_iterator = paginator.paginate()
    for page in page_iterator:
      for policy_desc in page['Policies']:
        policy_name_to_arn[policy_desc['PolicyName']] = policy_desc['Arn']
    if not policy_id in policy_name_to_arn:
      raise RuntimeError(f"AWS policy name '{policy_id}' was not found")
    policy_arn = policy_name_to_arn[policy_id]
  else:
    policy_arn = policy_id
  return policy_arn

def get_aws_step_policy(
      iam: IAMClient,
      policy_id: str,
    ) -> JsonableDict:
  policy_arn = get_aws_policy_arn(iam, policy_id)
  resp = iam.get_policy(PolicyArn=policy_arn)
  result = normalize_jsonable_dict(resp['Policy'])
  return result

def create_aws_step_policy(
      iam: IAMClient,
      policy_id: str,
      policy_document: Union[str, JsonableDict],
      description: Optional[str]=None,
      path: str='/service-role/',
      allow_exists: bool=False,
    ) -> JsonableDict:
  result: JsonableDict
  if allow_exists:
    try:
      result = get_aws_step_policy(iam, policy_id)
      return result
    except RuntimeError as ex:
      if not str(ex).endswith(' was not found'):
        raise

  if not isinstance(policy_document, str):
    policy_document = json.dumps(policy_document, sort_keys=True)
  if description is None:
    description = f"AWS step functions policy {policy_id}"
  resp = iam.create_policy(PolicyName=policy_id, PolicyDocument=policy_document, Path=path, Description=description)
  result = normalize_jsonable_dict(resp['Policy'])
  return result

def create_aws_step_role(
      iam: IAMClient,
      role_name: str,
      path: str='/service-role/',
      assume_role_policy_document: Optional[Union[str, JsonableDict]]=None,
      description: Optional[str]=None,
      max_session_duration: int=3600,
      permissions_boundary: Optional[JsonableDict]=None,
      add_policies: Optional[Dict[str, Optional[Union[str, JsonableDict]]]]=None,
      add_cloudwatch_policy: bool=True,
      add_xray_policy: bool=True,
      allow_exists: bool=False,
    ) -> JsonableDict:
  if description is None:
    description = f"Assumable role {role_name} for AWS stepfunctions"
  if assume_role_policy_document is None:
    assume_role_policy_document = DEFAULT_AWS_STEP_ASSUME_ROLE_POLICY_DOCUMENT
  if not isinstance(assume_role_policy_document, str):
    assume_role_policy_document = json.dumps(assume_role_policy_document, sort_keys=True)
  if add_policies is None:
    add_policies = {}
  else:
    add_policies = normalize_jsonable_dict(add_policies)
  if add_cloudwatch_policy:
    cloudwatch_policy_name = f"CloudwatchLogsDeliveryPolicy-{role_name}"
    add_policies[cloudwatch_policy_name] = DEFAULT_CLOUDWATCH_LOGS_DELIVERY_POLICY
  if add_xray_policy:
    xray_policy_name = f"XRayAccessPolicy-{role_name}"
    add_policies[xray_policy_name] = DEFAULT_XRAY_ACCESS_POLICY
  params = dict(
      RoleName=role_name,
      Path=path,
      Description=description,
      MaxSessionDuration=max_session_duration,
      AssumeRolePolicyDocument=assume_role_policy_document,
    )
  if not permissions_boundary is None:
    params.update(PermissionsBoundary=permissions_boundary)
  created: bool = False
  try:
    resp = iam.create_role(**params)
    created = True
  except iam.exceptions.EntityAlreadyExistsException:
    if not allow_exists:
      raise
    resp = iam.get_role(RoleName=role_name)
  if created:
    for policy_id, policy_document in add_policies.items():
      if policy_document is None:
        policy_arn = get_aws_policy_arn(iam, policy_id)
      else:
        policy_data = create_aws_step_policy(iam, policy_id, policy_document=policy_document, allow_exists=True )
        policy_arn = policy_data['Arn']
      iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
  result = normalize_jsonable_dict(resp['Role'])
  return result

# arn:aws:logs:us-west-2:745019234935:log-group:/aws/vendedlogs/states/HelloWorld-Logs:*
_log_group_arn_re = re.compile(r'^arn:aws:logs:(?P<region>[a-z][a-z0-9\-]+):(?P<account>[0-9]+):log-group:(?P<log_group_name>[^ \t\r\n<>{}[\]?*"#%\\^|~`$,;:/]+)(:(?P<tail>.*))?$')

def is_cloudwatch_log_group_arn(arn: str) -> bool:
  return not _log_group_arn_re.match(arn) is None

def get_cloudwatch_log_group_name_from_arn(arn: str) -> str:
  m = _log_group_arn_re.match(arn)
  if not m:
    raise RuntimeError(f'Invalid AWS CloudWatch log group ARN: "{arn}"')
  return m.group('log_group_name')

def get_cloudwatch_log_group_name_from_id(id: str) -> str:
  return id if not ':' in id else get_aws_step_activity_name_from_arn(id)

def find_cloudwatch_log_groups(
      logs: CloudWatchLogsClient,
      log_group_name_prefix: str,
    ) -> Dict[str, JsonableDict]:
  result: Dict[str, JsonableDict] = {}
  paginator = logs.get_paginator('describe_log_groups')
  page_iterator = paginator.paginate(logGroupNamePrefix=log_group_name_prefix)
  for page in page_iterator:
    for log_group_desc in page['logGroups']:
      result[log_group_desc['logGroupName']] = normalize_jsonable_dict(log_group_desc)
  return result

def get_cloudwatch_log_group(
      logs: CloudWatchLogsClient,
      log_group_id: str,
    ) -> JsonableDict:
  desc: JsonableDict
  log_group_name: str
  if ':' in log_group_id:
    log_group_name = get_cloudwatch_log_group_name_from_arn(log_group_id)
  else:
    log_group_name = log_group_id
  descs = find_cloudwatch_log_groups(logs, log_group_name)
  if not log_group_name in descs:
    raise RuntimeError(f"CloudWatch log group id '{log_group_id}' was not found")
  desc = descs[log_group_name]
  return desc

def create_cloudwatch_log_group(
      logs: CloudWatchLogsClient,
      log_group_id: str,
      allow_exists: bool=False,
    ) -> JsonableDict:
  log_group_name: str
  if ':' in log_group_id:
    log_group_name = get_cloudwatch_log_group_name_from_arn(log_group_id)
  else:
    log_group_name = log_group_id
  try:
    logs.create_log_group(logGroupName=log_group_name)
  except logs.exceptions.ResourceAlreadyExistsException:
    if not allow_exists:
      raise
  result = get_cloudwatch_log_group(logs, log_group_name)
  return result

def create_aws_step_state_machine(
      state_machine_id: str,
      states: Optional[Dict[str, JsonableDict]]=None,
      session: Optional[Session]=None,
      sfn: Optional[SFNClient]=None,
      iam: Optional[IAMClient]=None,
      logs: Optional[CloudWatchLogsClient]=None,
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
      allow_exists: bool=False
    ) -> JsonableDict:
  result: JsonableDict

  clients = dict(session=session, iam=iam, sfn=sfn, logs=logs)

  def get_session() -> Session:
    result: Optional[Session] = clients['session']
    if result is None:
      result = create_aws_session()
      clients['session'] = result
    return result

  def get_iam() -> IAMClient:
    result: Optional[IAMClient] = clients['iam']
    if result is None:
      result = get_session().client('iam')
      clients['iam'] = result
    return result

  def get_sfn() -> SFNClient:
    result: Optional[SFNClient] = clients['sfn']
    if result is None:
      result = get_session().client('stepfunctions')
      clients['sfn'] = result
    return result

  def get_logs() -> CloudWatchLogsClient:
    result: Optional[CloudWatchLogsClient] = clients['logs']
    if result is None:
      result = get_session().client('logs')
      clients['logs'] = result
    return result

  if allow_exists:
    try:
      result = describe_aws_step_state_machine(get_sfn(), state_machine_id)
      return result
    except RuntimeError as ex:
      if not str(ex).endswith('was not found'):
        raise

  if states is None:
    states = { start_at: dict(Type='Pass', End=True) }

  state_machine_name = state_machine_id

  if role_id is None:
    role_id = f'StepFunctions-{state_machine_name}-role'

  if role_description is None:
    role_description = f'Stepfunctions role for {role_id}'

  role_info = create_aws_step_role(
      get_iam(),
      role_name=role_id,
      path=role_path,
      assume_role_policy_document=assume_role_policy_document,
      description=role_description,
      max_session_duration=role_max_session_duration,
      permissions_boundary=role_permissions_boundary,
      add_policies=role_add_policies,
      add_cloudwatch_policy=role_add_cloudwatch_policy,
      add_xray_policy=role_add_xray_policy,
      allow_exists=allow_role_exists
    )
  role_arn = role_info['Arn']
  if comment is None:
    comment = f'Stepfunction state machine {state_machine_name}'
  definition: JsonableDict = dict(Comment=comment, StartAt=start_at, States=states)
  if not timeout_seconds is None and timeout_seconds != 0.0:
    definition['TimeoutSeconds'] = round(timeout_seconds)
  loggingConfiguration: LoggingConfigurationTypeDef = dict()
  if not loggingLevel is None:
    loggingConfiguration['level'] = str(loggingLevel)
  if not includeExecutionDataInLogs is None:
    loggingConfiguration['includeExecutionData'] = not not includeExecutionDataInLogs
  log_destinations: List[JsonableDict] = []
  if not loggingDestinations is None:
    log_destinations.extend(normalize_jsonable_list(loggingDestinations))
  default_log_group_arn: Optional[str] = None
  default_log_group_added: bool = False
  if add_default_cloudwatch_log_destination:
    default_log_group_name = f'/aws/vendedlogs/states/{state_machine_name}-Logs'
    default_log_group_desc = create_cloudwatch_log_group(get_logs(), default_log_group_name, allow_exists=True)
    default_log_group_arn = default_log_group_desc['arn']
  for destination in log_destinations:
    if 'cloudWatchLogsLogGroup' in destination:
      log_group_arn = destination['cloudWatchLogsLogGroup']['logGroupArn']
      if log_group_arn == default_log_group_arn:
        default_log_group_added = True
      else:
        create_cloudwatch_log_group(get_logs(), log_group_arn, allow_exists=True)
  if not default_log_group_arn is None and not default_log_group_added:
    log_destinations.append(dict(cloudWatchLogsLogGroup=dict(logGroupArn=default_log_group_arn)))
  if len(log_destinations) > 0:
    loggingConfiguration['destinations'] = log_destinations

  tracingConfiguration: TracingConfigurationTypeDef = dict(enabled=not not tracingEnabled)

  logger.debug(f'Creating state machine "{state_machine_name}"; roleArn="{role_arn}"; definition={json.dumps(definition, sort_keys=True)}')

  # Unfortunately create_state_machine will fail if the role has just been created and the attached
  # policies have not had time to propogate.  So, we will retry for a little while.
  retry_count: int = 0
  while True:
    try:
      resp = get_sfn().create_state_machine(
          name=state_machine_name,
          definition=json.dumps(definition, sort_keys=True),
          roleArn=role_arn,
          type=state_machine_type,
          loggingConfiguration=loggingConfiguration,
          tracingConfiguration=tracingConfiguration,
        )
      break
    except ClientError as ex:
      if retry_count > 6 or ex.response['Error']['Code'] != 'AccessDeniedException':
        logger.warning(f'Error creating state machine, retry_count={retry_count}, exception class={full_type(ex)}, Error code="{ex.response["Error"]["Code"]}", roleArn="{role_arn}",definition={json.dumps(definition, sort_keys=True)}"')
        raise
      logger.info(f'Access denied creating state machine, possible AWS still syncing policies; retry_count={retry_count}, exception class={full_type(ex)}, Error code="{ex.response["Error"]["Code"]}", roleArn="{role_arn}",definition={json.dumps(definition, sort_keys=True)}"')
      retry_count += 1
      time.sleep(3)

  state_machine_arn = resp['stateMachineArn']
  result = describe_aws_step_state_machine(get_sfn(), state_machine_arn)
  return result

def describe_aws_step_activity(
      sfn: SFNClient,
      activity_id: str,
    ) -> JsonableDict:
  """Returns the result of SFNClient.describe_activity() for the given activity name or ARN.

  Args:
      sfn (SFNClient): The boto3 client for AWS step functions
      activity_id (str): Either an AWS step function activity name, or its ARN

  Raises:
      RuntimeError: The specified activity_id does not exist.

  Returns:
      JsonableDict: a dict with 'creationDate', 'name' and 'activityArn' fields, similar to t
                    the result of sfn.describe_activity for the requested ID.
  """
  activity_arn: str
  activity_name: str
  result: JsonableDict
  if ':' in activity_id:
    # activity_id must be an ARN. Look up the activity name
    resp = sfn.describe_activity(activityArn=activity_id)
    result = dict(
        name=resp['name'],
        activityArn=resp['activityArn'],
        creationDate=str(resp['creationDate'])
      )
  else:
    # activity_id must be an activity name. Enumerate all activities and
    # find the matching name
    activity_name_to_entry: Dict[str, JsonableDict] = {}
    paginator = sfn.get_paginator('list_activities')
    page_iterator = paginator.paginate()
    for page in page_iterator:
      for act_desc in page['activities']:
        activity_name_to_entry[act_desc['name']] = dict(
            name=act_desc['name'],
            activityArn=act_desc['activityArn'],
            creationDate=str(act_desc['creationDate'])
          )
    if not activity_id in activity_name_to_entry:
      raise RuntimeError(f"AWS stepfunctions activity name '{activity_id}' was not found")
    result = activity_name_to_entry[activity_id]
  return result

def create_aws_step_activity(
      sfn: SFNClient,
      activity_id: str,
      allow_exists: bool=True
    ) -> JsonableDict:
  """Creates an activity that can implement a state of an AWS step function.

  Args:
      sfn (SFNClient): The boto3 client for AWS step functions
      activity_id (str): The unique (within this account) AWS step function activity name
      allow_exists (bool, Optional): Suppress error if activity exists. The default is True

  Returns:
      JsonableDict: a dict with 'creationDate', 'name' and 'activityArn' fields, similar to
                    the result of sfn.describe_activity for the new activity.
  """
  if allow_exists:
    try:
      result = describe_aws_step_activity(sfn, activity_id)
      return result
    except RuntimeError as ex:
      if ':' in activity_id or not str(ex).endswith('was not found'):
        raise
  result = normalize_jsonable_dict(sfn.create_activity(name=activity_id))
  result['name'] = activity_id
  return result

def delete_aws_step_activity(
      sfn: SFNClient,
      activity_id: str,
      must_exist: bool=False
    ):
  """Creates an activity that can implement a state of an AWS step function.

  Args:
      sfn (SFNClient): The boto3 client for AWS step functions
      activity_id (str): Either an AWS step function activity name, or its ARN
      must_exist (bool, optional): Raise an execption if the activity does not exist. Default is False.
  """
  activity_arn: str
  if must_exist or not ':' in activity_id:
    desc = describe_aws_step_activity(sfn, activity_id)
    activity_arn = desc['activityArn']
  else:
    activity_arn = activity_id
  sfn.delete_activity(activityARn=activity_arn)

_activity_arn_re = re.compile(r'^arn:aws:states:(?P<region>[a-z][a-z0-9\-]+):(?P<account>[0-9]+):activity:(?P<activity_name>[^ \t\r\n<>{}[\]?*"#%\\^|~`$,;:/]+)$')

def is_aws_step_activity_arn(arn: str) -> bool:
  return not _activity_arn_re.match(arn) is None

def get_aws_step_activity_name_from_arn(arn: str) -> str:
  m = _activity_arn_re.match(arn)
  if not m:
    raise RuntimeError(f'Invalid AWS stepfunction activity ARN: "{arn}"')
  return m.group('activity_name')

# arn:aws:states:us-west-2:745019234935:execution:TestRunSelectedActivity:9de2711e-06a3-44d2-b95e-9a93599bd1f8
_execution_arn_re = re.compile(r'^arn:aws:states:(?P<region>[a-z][a-z0-9\-]+):(?P<account>[0-9]+):execution:(?P<state_machine_name>[^ \t\r\n<>{}[\]?*"#%\\^|~`$,;:/]+):(?P<execution_name>[^ \t\r\n<>{}[\]?*"#%\\^|~`$,;:/]+)$')

def is_aws_step_execution_arn(arn: str) -> bool:
  return not _execution_arn_re.match(arn) is None

def get_aws_step_state_machine_and_execution_names_from_arn(arn: str) -> Tuple[str, str]:
  m = _execution_arn_re.match(arn)
  if not m:
    raise RuntimeError(f'Invalid AWS stepfunction execution ARN: "{arn}"')
  return m.group('state_machine_name'), m.group('execution_name')

# arn:aws:states:us-west-2:745019234935:stateMachine:TestRunSelectedActivity
_state_machine_arn_re = re.compile(r'^arn:aws:states:(?P<region>[a-z][a-z0-9\-]+):(?P<account>[0-9]+):stateMachine:(?P<state_machine_name>[^ \t\r\n<>{}[\]?*"#%\\^|~`$,;:/]+)$')

def is_aws_step_state_machine_arn(arn: str) -> bool:
  return not _state_machine_arn_re.match(arn) is None

def get_aws_step_state_machine_name_from_arn(arn: str) -> str:
  m = _state_machine_arn_re.match(arn)
  if not m:
    raise RuntimeError(f'Invalid AWS stepfunction state machine ARN: "{arn}"')
  return m.group('state_machine_name')

def describe_aws_step_execution(
      sfn: SFNClient,
      execution_id: str,
      state_machine_id: Optional[str]=None,
    ) -> JsonableDict:
  """Returns the result of SFNClient.describe_execution() for the given state machine name or ARN.

  Args:
      sfn (SFNClient):
          The boto3 client for AWS step functions
      state_machine_id (str):
          Either an AWS step function execution name, or its ARN
      state_machine_id (str):
         Either an AWS step function state machine name, or its ARN. May be None
         if state_machine_id is an ARN. Default is None.

  Raises:
      RuntimeError: The specified execution_id does not exist.
      RuntimeError: The specified state_machine_id does not exist.

  Returns:
      JsonableDict: a dict as returned by SFNClient.describe_execution; e.g.:

          {
              "executionArn": "arn:aws:states:us-west-2:745019234935:execution:TestRunSelectedActivity:9de2711e-06a3-44d2-b95e-9a93599bd1f8",
              "stateMachineArn": "arn:aws:states:us-west-2:745019234935:stateMachine:TestRunSelectedActivity",
              "name": "9de2711e-06a3-44d2-b95e-9a93599bd1f8",
              "status": "TIMED_OUT",
              "startDate": "2022-10-09T17:36:00.742000-07:00",
              "stopDate": "2022-10-09T17:46:00.743000-07:00",
              "input": "{ \"Comment\": \"foo\" }",
              "inputDetails": {
                  "included": true
              },
              "traceHeader": "Root=1-634368f0-fd3bef0eed732a65fb6e51fb;Sampled=1"
          }
  """
  execution_arn: str
  if ':' in execution_id:
    execution_arn = execution_id
  else:
    state_machine_arn: str
    if state_machine_id is None:
      raise RuntimeError("state_machine_id must be provided if execution_id is not an ARN")
    if ':' in state_machine_id:
      # state_machine_id must be an ARN.
      state_machine_arn = state_machine_id
    else:
      # state_machine_id must be a state machine name. Enumerate all state machines and
      # find the matching name
      state_machine_name_to_arn: Dict[str, str] = {}
      paginator = sfn.get_paginator('list_state_machines')
      page_iterator = paginator.paginate()
      for page in page_iterator:
        for state_machine_desc in page['stateMachines']:
          state_machine_name_to_arn[state_machine_desc['name']] = state_machine_desc['stateMachineArn']
      if not state_machine_id in state_machine_name_to_arn:
        raise RuntimeError(f"AWS stepfunctions state machine name '{state_machine_id}' was not found")
      state_machine_arn = state_machine_name_to_arn[state_machine_id]
    m = _state_machine_arn_re.match(state_machine_arn)
    if not m:
      raise RuntimeError(f'Invalid AWS stepfunction state machine ARN: "{state_machine_arn}"')
    state_machine_name = m.group('state_machine_name')
    aws_region = m.group('region')
    aws_account = m.group('account')
    execution_arn=f'arn:aws:states:{aws_region}:{aws_account}:execution:{state_machine_name}:{execution_id}'

  resp = sfn.describe_execution(executionArn=execution_arn)
  result = normalize_jsonable_dict(resp)

  return result
