# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""General utility functions for this package"""

from .logging import logger

from typing import TYPE_CHECKING, Optional, Type, Any, Dict
from .internal_types import Jsonable, JsonableDict, JsonableList, STSClient
from collections.abc import Mapping, Iterable

import boto3
import botocore
import botocore.session
from boto3 import Session

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

def get_aws_caller_identity(
      session: Optional[Session]=None,
      sts: Optional[STSClient]=None,
      aws_access_key_id: Optional[str]=None,
      aws_secret_access_key: Optional[str]=None,
      aws_session_token: Optional[str]=None,
      region_name: Optional[str]=None,
      botocore_session: Optional[botocore.session.Session]=None,
      profile_name: Optional[str]=None,
    ) -> JsonableDict:
  if sts is None:
    if session is None:
      session = create_aws_session(
          aws_access_key_id=aws_access_key_id,
          aws_secret_access_key=aws_secret_access_key,
          aws_session_token=aws_session_token,
          region_name=region_name,
          botocore_session=botocore_session,
          profile_name=profile_name
      )
    sts = session.client('sts')
  resp = sts.get_caller_identity()
  return normalize_jsonable_dict(resp)

def get_aws_account(
      session: Optional[Session]=None,
      sts: Optional[STSClient]=None,
      aws_access_key_id: Optional[str]=None,
      aws_secret_access_key: Optional[str]=None,
      aws_session_token: Optional[str]=None,
      region_name: Optional[str]=None,
      botocore_session: Optional[botocore.session.Session]=None,
      profile_name: Optional[str]=None,
    ) -> str:
  """Returns the AWS account number string associated with the AWS session

  Args:
      session (Optional[Session], optional):
          The AWS session to use. Ignored if sts is provided. If None, a session is created
          using the remaining parameters. Defaults to None.
      sts (Optional[STSClient], optional): The AWS secure token service client to use.
          If None, a new client is created from the AWS session. Defaults to None.
      aws_access_key_id (Optional[str], optional):
          The AWS access key ID. Ignored if sts or session is provided. If None,
          the value is determined from the profile or environment vars. Defaults to None.
      aws_secret_access_key (Optional[str], optional):
          The AWS secret access key. Ignored if sts or session is provided. If None,
          the value is determined from the profile or environment vars. Defaults to None.
      aws_session_token (Optional[str], optional):
          The AWS session token. Ignored if sts or session is provided. If None,
          the value is determined from the profile or environment vars. Defaults to None.
      region_name (Optional[str], optional):
          The AWS region name. Ignored if sts or session is provided. If None,
          the value is determined from the profile or environment vars. Defaults to None.
      botocore_session (Optional[botocore.session.Session], optional):
          The botocore session to use as the basis for a session. Ignored if sts or session is provided. If None,
          a new session is created from defaults and other parameters. Defaults to None.
      profile_name (Optional[str], optional):
          The AWS profile name. Ignored if sts or session is provided. If None,
          the value is determined from environment variables, defaulting to 'default'. Defaults to None.

  Returns:
      str: The AWS account number expressed as a string
  """
  caller_identity = get_aws_caller_identity(
      session=session,
      sts=sts,
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      aws_session_token=aws_session_token,
      region_name=region_name,
      botocore_session=botocore_session,
      profile_name=profile_name,
    )
  return caller_identity['Account']


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

def normalize_jsonable(value: any) -> Jsonable:
  """Presents an object as a simple JSON-serializable value, recursively.
  
  Simple Jsonable scalar values (including None) are preserved.
  Mappable objects are converted to simple dicts, with string keys and normalized child values.
  Iterable objects are converted to simple lists with normalized elements.
  All other values are converted to simple strings.


  Args:
      value (any): Any value, interpreted as described above

  Returns:
      Jsonable: A value that can be round-trip converted to JSON and back.
  """
  if value is None or isinstance(value, (str, float, int, bool)):
    result = value
  elif isinstance(value, Mapping):
    result: JsonableDict = dict((str(k), normalize_jsonable(v)) for k, v in value.items())
  elif isinstance(value, Iterable):
    result: JsonableList = [ normalize_jsonable(x) for x in value ]
  else:
    result = str(value)

  return result  

def normalize_jsonable_dict(value: Mapping) -> JsonableDict:
  """Presents an object as a simple JSON-serializable dict, recursively.
  
  Simple Jsonable scalar values (including None) are preserved.
  Mappable objects are converted to simple dicts, with string keys and normalized child values.
  Iterable objects are converted to simple lists with normalized elements.
  All other values are converted to simple strings.


  Args:
      value (Mapping): Any dict-like value, interpreted as described above

  Raises:
      ValueError: The provided value is not a Mapping

  Returns:
      JsonablezDict: A dict value that can be round-trip converted to JSON and back.
  """
  result = normalize_jsonable(value)
  if not isinstance(result, dict):
    raise ValueError(f"Value is not dict-like: {value}")

  return result  

def normalize_jsonable_list(value: Iterable) -> JsonableList:
  """Presents an object as a simple JSON-serializable list, recursively.
  
  Simple Jsonable scalar values (including None) are preserved.
  Mappable objects are converted to simple dicts, with string keys and normalized child values.
  Iterable objects are converted to simple lists with normalized elements.
  All other values are converted to simple strings.


  Args:
      value (Iterable): Any list-like value, interpreted as described above

  Raises:
      ValueError: The provided value is not Iterable

  Returns:
      JsonableList: A list value that can be round-trip converted to JSON and back.
  """
  result = normalize_jsonable(value)
  if not isinstance(result, list):
    raise ValueError(f"Value is not list-like: {value}")

  return result  
