# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

from .logging import logger

from genericpath import isfile
from typing import Optional, Type, Any, Dict, Tuple, Generator, IO, List
from .internal_types import Jsonable

import os
import sys
import boto3
import botocore
import botocore.session
from boto3 import Session
from mypy_boto3_s3.client import S3Client, Exceptions as S3Exceptions
from mypy_boto3_s3.type_defs import ObjectTypeDef
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import urllib.parse

from .util import create_aws_session

from aws_step_activity.internal_types import JsonableDict

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
