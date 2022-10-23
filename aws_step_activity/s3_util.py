# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""General utility functions for working with AWS S3"""

from concurrent.futures.process import _ResultItem
from dataclasses import field
from .logging import logger

from typing import Optional, Type, Any, Dict, Tuple, Generator, IO, List, Union
from .internal_types import Jsonable, JsonableDict, S3Client, S3_ObjectTypeDef as ObjectTypeDef

import os
import sys
import boto3
import botocore
import botocore.session
from boto3 import Session
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import urllib.parse
import requests
from io import StringIO
from io import BytesIO

from .util import create_aws_session, full_type, normalize_jsonable_dict

def is_s3_url(url: str) -> bool:
  return url.startswith('s3:')

def parse_s3_url(url: str) -> Tuple[str, str]:
  parsed = urlparse(url, allow_fragments=False)
  if parsed.scheme != 's3':
    raise ValueError(f"Invalid S3 URL: {url}")
  bucket = parsed.netloc
  key = parsed.path.lstrip('/')
  if parsed.query:
    key += '?' + parsed.query
  return bucket, key

def create_s3_url(bucket: str, key: str) -> str:
  key = key.lstrip('/')
  url = 's3://' + bucket
  if key != '':
    url += '/' + key
  return url

def get_s3(
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ):
  if s3 is None:
    if session is None:
      session = create_aws_session(region_name=aws_region, profile_name=aws_profile)
    s3 = session.client('s3')
  return s3

def s3_object_infos_under_path(
      url: str,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None,
      allow_nonfolder: bool = True
    ) -> Generator[ObjectTypeDef, None, None]:
  bucket, top_key = parse_s3_url(url)
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  if allow_nonfolder and top_key != '' and not top_key.endswith('/'):
    try:
      resp = s3.head_object(Bucket=bucket, Key=top_key)
      obj_desc: ObjectTypeDef = dict(
          Key=top_key,
          LastModified=resp['LastModified'],
          ETag=resp['ETag'],
          Size=resp['ContentLength'],
          StorageClass=resp.get('StorageClass', 'STANDARD'),
        )
      yield obj_desc
    except ClientError as ex:
      if not str(ex).endswith(': Not Found'):
        raise
  paginator = s3.get_paginator('list_objects_v2')
  prefix = top_key
  if prefix != '' and not prefix.endswith('/'):
    prefix = prefix + '/'

  page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
  for page in page_iterator:
    if 'Contents' in page:
      for obj_desc in page['Contents']:
        yield obj_desc
  

def s3_object_urls_under_path(
      url: str,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None,
      allow_nonfolder: bool = True
    ):
  bucket, top_key = parse_s3_url(url)
  for object_info in s3_object_infos_under_path(
        url,
        s3=s3,
        session=session,
        aws_profile=aws_profile,
        aws_region=aws_region,
        allow_nonfolder=allow_nonfolder
      ):
    key = object_info['Key']
    if not key.endswith('/'):
      yield create_s3_url(bucket, key)

def s3_download_object_to_file(
      url: str,
      filename: Optional[str]=None,
      output_dir: Optional[str]=None,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ) -> str:
  bucket, key = parse_s3_url(url)
  if filename is None:
    filename = os.path.basename(key)
  if output_dir is None:
    output_dir = '.'
  filename = os.path.abspath(os.path.join(os.getcwd(), output_dir, filename))
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  
  s3.download_file(
      Bucket=bucket,
      Key=key,
      Filename=filename
    )
  return filename

def s3_download_object_to_fileobj(
      url: str,
      f: IO,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ):
  bucket, key = parse_s3_url(url)
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  s3.download_fileobj(
      Bucket=bucket,
      Key=key,
      Fileobj=f
    )

def s3_upload_file_to_object(
      url: str,
      filename: str,
      cwd: Optional[str]=None,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ) -> str:
  bucket, key = parse_s3_url(url)
  if cwd is None:
    cwd = '.'
  filename = os.path.abspath(os.path.join(os.getcwd(), cwd, filename))
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  
  s3.upload_file(
      Filename=filename,
      Bucket=bucket,
      Key=key
    )

def s3_download_folder(
      url: str,
      output_folder: str,
      cwd: Optional[str]=None,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ):
  bucket, key = parse_s3_url(url.rstrip('/'))
  url_prefix = create_s3_url(bucket, key) + '/'
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  if cwd is None:
    cwd = '.'
  output_folder = os.path.abspath(os.path.join(os.getcwd(), cwd, output_folder))
  parent_folder = os.path.dirname(output_folder)
  if not os.path.isdir(parent_folder):
    raise RuntimeError(f'Directory {parent_folder} does not exist')
  for s3_url in s3_object_urls_under_path(url, s3=s3, allow_nonfolder=False):
    if not s3_url.startswith(url_prefix):
      raise RuntimeError(f'Unexpected S3 URL "{s3_url}" does not match prefix "{url_prefix}"')
    local_rel_path = s3_url[len(url_prefix):]
    local_path = os.path.join(output_folder, local_rel_path)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_download_object_to_file(s3_url, filename=local_path, s3=s3)

def files_in_folder(
      folder: str,
      cwd: Optional[str]=None,
    ) -> List[str]:
  if cwd is None:
    cwd = '.'
  folder = os.path.abspath(os.path.join(os.getcwd(), cwd, folder))
  if not os.path.isdir(folder):
    raise RuntimeError(f'Directory does not exist: {folder}')

  results: List[str] = []
  def add_subdir(subdir: str):
    for entry in os.listdir(subdir):
      entry_path = os.path.join(subdir, entry)
      if os.path.isfile(entry_path):
        entry_relpath = os.path.relpath(entry_path, folder)
        results.append(entry_relpath)
      elif os.path.isdir(entry_path):
        add_subdir(entry_path)
  
  add_subdir(folder)
  return sorted(results)

def s3_upload_folder(
      url: str,
      input_folder: str,
      cwd: Optional[str]=None,
      s3: Optional[S3Client]=None,
      session: Optional[Session]=None,
      aws_profile: Optional[str]=None,
      aws_region: Optional[str]=None
    ):
  if cwd is None:
    cwd = '.'
  input_folder = os.path.abspath(os.path.join(os.getcwd(), cwd, input_folder))
  if not os.path.isdir(input_folder):
    raise RuntimeError(f'Directory does not exist: {input_folder}')
  bucket, key = parse_s3_url(url.rstrip('/'))
  url_prefix = create_s3_url(bucket, key) + '/'
  s3 = get_s3(
      s3=s3,
      session=session,
      aws_profile=aws_profile,
      aws_region=aws_region
    )
  for rel_file in files_in_folder(input_folder):
    abs_file = os.path.join(input_folder, rel_file)
    object_url = url_prefix + rel_file
    s3_upload_file_to_object(object_url, abs_file, s3=s3)

def generate_presigned_s3_upload_post(
      s3: S3Client,
      s3_object_url: str,
      expiration_seconds: int=600,
    ) -> JsonableDict:
  """For a given S3 object URI, generates metadata for a temporary signed upload
     HTTP POST request that will not require AWS credentials to succeed.

     This can be used, e.g., by an API server to provide the client with the
     ability to directly upload a file to S3 without having to stream the
     contents of the file through the API server.

     The result is of the form:
        {
          "fields": {
            "AWSAccessKeyId": "<aws-access-key-id>",
            "key": "<s3-object-key>",
            "policy": "<base64-encoded-aws-policy>",
            "signature": "<base64-encoded-signature>",
            ...
          },
          "url": "https://<s3-bucket-name>.s3.amazonaws.com/"
        }

     An upload POST can be constructed using the result with curl as:

       curl \
         -F AWSAccessKeyId='<aws-access-key-id>' \
         -F key='<s3-object-key> \
         -F policy='<base64-encoded-aws-policy' \
         -F signature='<base64-encoded-signature>' \
         -F file=@<local-file-pathname> \
         https://<s3-bucket-name>.s3.amazonaws.com/

     An upload POST can be constructed in Python using the "requests"
     module with:

        import requests
        import os

        # local_filename: str = <path to local file to upload>
        # signed_post: JsonableDict = <result of generate_presigned_s3_upload_post()>

        with open(local_filename, 'rb') as fd:
          r = requests.post(
              signed_post['url'],
              data=signed_post['fields'],
              files={ "file": (os.path.basename(local_filename), fd) },
            )
        r.raise_for_status()

     Functions upload_to_s3_with_signed_post(), upload_file_to_s3_with_signed_post()
     and upload_data_to_s3_with_signed_post() are provided in this module to
     properly invoke a signed upload POST.

  Args:
      s3 (S3Client):
          The boto3 S3 client in the desired AWS session
      s3_object_url (str):
          The "s3://" URL for the object to be uploaded. The object may or may not exist.
      expiration_seconds (int, optional):
          The maximum number of seconds during which the metadata may be used to upload
          to the specified S3 object. Defaults to 600 (ten minutes).

  Returns:
      JsonableDict:
          A JSON-friendly dict that is used to construct an upload HTTP POST request.
  """
  bucket, key = parse_s3_url(s3_object_url)

  resp = s3.generate_presigned_post(
      Bucket=bucket,
      Key=key,
      ExpiresIn=expiration_seconds
    )
  result = normalize_jsonable_dict(resp)
  return result

def upload_to_s3_with_signed_post(
      signed_post: JsonableDict,
      fd: IO,
      filename: Optional[str]=None,
      fields: Optional[JsonableDict]=None,
      headers: Optional[Dict[str, str]]=None,
    ) -> None:
  """Upload the contents of a file-like object to S3 using a presigned POST

  Args:
      signed_post (JsonableDict):
          Presigned POST metadata as returned from generate_presigned_s3_upload_post()
      fd (IO): A readable file-like object.
          StringIO or BinaryIO are supported, but binary is recommended
      filename (str, optional):
          The logical name of the uploaded file as presented to S3. If None, uses the
          basename of the S3 object key. Defaults to None.
      fields (Optional[JsonableDict], optional):
          Optional dictionary of additional fields to POST. Defaults to None.
      headers (Optional[Dict[str, str]], optional):
          Optional additional HTTP headers to POST. Defaults to None.
  """
  post_url: str = signed_post['url']
  data: JsonableDict = {}
  if not fields is None:
    data.update(fields)
  data.update(signed_post['fields'])
  if filename is None:
    filename = data['key']
  files = dict(file=(os.path.basename(filename), fd))
  r = requests.post(
      post_url,
      data=data,
      files=files,
      headers=headers)
  r.raise_for_status()

def upload_file_to_s3_with_signed_post(
      signed_post: JsonableDict,
      filename: str,
      fields: Optional[JsonableDict]=None,
      uploaded_filename: Optional[str]=None,
      headers: Optional[Dict[str, str]]=None,
    ) -> None:
  """Upload the contents of a named file to S3 using a presigned POST

  Args:
      signed_post (JsonableDict):
          Presigned POST metadata as returned from generate_presigned_s3_upload_post()
      filename (str, optional):
          The local pathname of a file to be uploaded to S3.
      fields (Optional[JsonableDict], optional):
          Optional dictionary of additional fields to POST. Defaults to None.
      uploaded_filename (str, optional):
          The logical name of the uploaded file as presented to S3. If None, uses the
          basename of file being uploaded. Defaults to None.
      headers (Optional[Dict[str, str]], optional):
          Optional additional HTTP headers to POST. Defaults to None.
  """
  if uploaded_filename is None:
    uploaded_filename = os.path.basename(filename)
  with open(filename, 'rb') as fd:
    upload_to_s3_with_signed_post(
        signed_post,
        fd,
        filename=uploaded_filename,
        fields=fields, 
        headers=headers
      )

def upload_data_to_s3_with_signed_post(
      signed_post: JsonableDict,
      data: Union[str, bytes],
      filename: Optional[str]=None,
      fields: Optional[JsonableDict]=None,
      headers: Optional[Dict[str, str]]=None,
    ) -> None:
  """Uploads an S3 object with content from a string or bytes value, using a presigned POST

  Args:
      signed_post (JsonableDict):
          Presigned POST metadata as returned from generate_presigned_s3_upload_post()
      data (Union[str, bytes]):
          A string or bytes value containing the content to be used for upload
      filename (str, optional):
          The logical name of the uploaded file as presented to S3. If None, uses the
          basename of the S3 object key. Defaults to None.
      fields (Optional[JsonableDict], optional):
          Optional dictionary of additional fields to POST. Defaults to None.
      headers (Optional[Dict[str, str]], optional):
          Optional additional HTTP headers to POST. Defaults to None.

  Raises:
      TypeError: The provided data is not a string or bytes value
  """
  if isinstance(data, bytes):
    fd = BytesIO(data)
  elif isinstance(data, str):
    fd = StringIO(data)
  else:
    raise TypeError(f"data must be str or bytes: {full_type(data)}")
  with fd:
    upload_to_s3_with_signed_post(
        signed_post,
        fd,
        filename=filename,
        fields=fields,
        headers=headers
      )
