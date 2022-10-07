# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""General utility functions for working with AWS step functions"""

from .logging import logger

from typing import Optional, Type, Any, Dict, Tuple, Generator, IO, List
from .internal_types import Jsonable, JsonableDict

import os
import sys
import boto3
import botocore
import botocore.session
from boto3 import Session
from mypy_boto3_s3.client import S3Client, Exceptions as S3Exceptions
from mypy_boto3_s3.type_defs import ObjectTypeDef
from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions

from botocore.exceptions import ClientError
from urllib.parse import urlparse
import urllib.parse

from .util import create_aws_session, normalize_jsonable_dict

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


def describe_aws_step_activity(
      sfn: SFNClient,
      activity_id: str
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
      activity_name: str
    ) -> JsonableDict:
  """Creates an activity that can implement a state of an AWS step function.

  Args:
      sfn (SFNClient): The boto3 client for AWS step functions
      activity_name (str): The unique (within this account) AWS step function activity name

  Returns:
      JsonableDict: a dict with 'creationDate', 'name' and 'activityArn' fields, similar to
                    the result of sfn.describe_activity for the new activity.
  """
  result = normalize_jsonable_dict(sfn.create_activity(name=activity_name))
  result['name'] = activity_name
  return result
