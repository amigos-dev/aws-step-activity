# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

from typing import Optional

import boto3
import botocore
import botocore.session
from boto3 import Session

def CreateSession(
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
    
  