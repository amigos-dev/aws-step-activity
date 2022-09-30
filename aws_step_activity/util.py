# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

from typing import Optional, Type, Any, Dict

import boto3
import botocore
import botocore.session
from boto3 import Session
from mypy_boto3_stepfunctions.client import SFNClient, Exceptions as SFNExceptions

from aws_step_activity.internal_types import JsonableDict

def create_aws_session(
      session: Optional[Session]=None,
      aws_access_key_id: Optional[str]=None,
      aws_secret_access_key: Optional[str]=None,
      aws_session_token: Optional[str]=None,
      region_name: Optional[str]=None,
      botocore_session: Optional[botocore.session.Session]=None,
      profile_name: Optional[str]=None,
    ) -> Session:
  """Create a new boto3 session, optionally using an existing session as a template.

  Args:
      session (Optional[Session], optional): Existing boto3 session to use as a base. Defaults to None.
      aws_access_key_id (Optional[str], optional): AWS access key ID, overriding base or profile. Defaults to None.
      aws_secret_access_key (Optional[str], optional): AWS secret access key, overriding base or profile. Defaults to None.
      aws_session_token (Optional[str], optional): AWS session token, overriding base or profile. Defaults to None.
      region_name (Optional[str], optional): AWS region name, overriding base or profile. Defaults to None.
      botocore_session (Optional[botocore.session.Session], optional): Optional botocore session. Defaults to None.
      profile_name (Optional[str], optional): AWS profile name, overriding base or default profile. Defaults to None.

  Returns:
      Session: A new boto3 session
  """
  if not session is None:
    if aws_access_key_id is None:
      aws_access_key_id = session.get_credentials().access_key
    if aws_secret_access_key is None:
      aws_secret_access_key = session.get_credentials().secret_key
    if aws_session_token is None:
      aws_session_token = session.get_credentials().token
    if region_name is None:
      region_name = session.region_name
    if profile_name is None:
      profile_name = session.profile_name
  
  new_session = Session(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      aws_session_token=aws_session_token,
      region_name=region_name,
      botocore_session=botocore_session,
      profile_name=profile_name
  )

  return new_session
    
def full_name_of_type(t: Type) -> str:
  """Returns the fully qualified name of a python type

  Args:
      t (Type): A python type, which may be a builtin type or a class

  Returns:
      str: The fully qualified name of the type, including the package/module
  """
  module: str = t.__module__
  if module == 'builtins':
    result: str = t.__qualname__
  else:
    result = module + '.' + t.__qualname__
  return result

def full_type(o: Any) -> str:
  """Returns the fully qualified name of an object or value's type

  Args:
      o: any object or value

  Returns:
      str: The fully qualified name of the object or value's type,
           including the package/module
  """
  return full_name_of_type(o.__class__)

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
  if '/' in activity_id:
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
