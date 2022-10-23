# Copyright (c) 2022 Samuel J. McKelvie
#
# MIT License - See LICENSE file accompanying this package.
#

"""Command-line interface for this package"""

import base64
from math import exp
from typing import (
    TYPE_CHECKING, Optional, Sequence, List, Union, Dict, TextIO, Mapping, MutableMapping,
    cast, Any, Iterator, Iterable, Tuple, ItemsView, ValuesView, KeysView, Type, IO )

import logging
import uuid
from .logging import logger

import os
import sys
import datetime
import argparse
#import argcomplete # type: ignore[import]
import json
from base64 import b64encode, b64decode
import colorama # type: ignore[import]
from colorama import Fore, Back, Style
import subprocess
from io import TextIOWrapper
import yaml
from urllib.parse import urlparse, ParseResult
import ruamel.yaml # type: ignore[import]
from io import StringIO

from aws_step_activity.worker import AwsStepActivityWorker

from .exceptions import AwsStepActivityError
from .internal_types import JsonableTypes, Jsonable, JsonableDict, JsonableList, SFNClient
from .version import __version__ as pkg_version
from .util import full_type, create_aws_session
from .sfn_util import describe_aws_step_activity, get_aws_step_state_machine_name_from_arn
from .s3_util import (
    S3Client,
    generate_presigned_s3_upload_post,
    upload_file_to_s3_with_signed_post,
    s3_upload_folder
  )
from .state_machine import AwsStepStateMachine
from boto3 import Session

def is_colorizable(stream: TextIO) -> bool:
  is_a_tty = hasattr(stream, 'isatty') and stream.isatty()
  return is_a_tty


class CmdExitError(RuntimeError):
  exit_code: int

  def __init__(self, exit_code: int, msg: Optional[str]=None):
    if msg is None:
      msg = f"Command exited with return code {exit_code}"
    super().__init__(msg)
    self.exit_code = exit_code

class ArgparseExitError(CmdExitError):
  pass

class NoExitArgumentParser(argparse.ArgumentParser):
  def exit(self, status=0, message=None):
    if message:
      self._print_message(message, sys.stderr)
    raise ArgparseExitError(status, message)


class CommandLineInterface:
  _argv: Optional[Sequence[str]]
  _parser: argparse.ArgumentParser
  _args: argparse.Namespace
  _cwd: str

  _raw_stdout: TextIO = sys.stdout
  _raw_stderr: TextIO = sys.stderr
  _raw: bool = False
  _compact: bool = False
  _output_file: Optional[str] = None
  _encoding: str = 'utf-8'

  _colorize_stdout: bool = False
  _colorize_stderr: bool = False

  _aws_session: Optional[Session] = None
  _sfn: Optional[SFNClient] = None
  _s3: Optional[S3Client] = None
  _activity_id: Optional[str] = None
  _state_machine_id: Optional[str] = None
  _state_machine: Optional[AwsStepStateMachine] = None
  _s3_base_url: Optional[str] = None
  _have_s3_base_url: bool = False

  def __init__(self, argv: Optional[Sequence[str]]=None):
    self._argv = argv

  def ocolor(self, codes: str) -> str:
    return codes if self._colorize_stdout else ""

  def ecolor(self, codes: str) -> str:
    return codes if self._colorize_stderr else ""

  @property
  def cwd(self) -> str:
    return self._cwd

  def abspath(self, path: str) -> str:
    return os.path.abspath(os.path.join(self.cwd, os.path.expanduser(path)))

  def get_aws_session(self) -> Session:
    if self._aws_session is None:
      self._aws_session = create_aws_session(profile_name=self._args.aws_profile, region_name=self._args.aws_region)
    return self._aws_session

  def get_sfn(self) -> SFNClient:
    if self._sfn is None:
      self._sfn = self.get_aws_session().client('stepfunctions')
    return self._sfn

  def get_s3(self) -> S3Client:
    if self._s3 is None:
      self._s3 = self.get_aws_session().client('s3')
    return self._s3

  def get_activity_id(self) -> str:
    if self._activity_id is None:
      activity_id: Optional[str] = self._args.activity_id
      if activity_id is None:
        activity_id = os.environ.get('AWS_STEP_ACTIVITY_ID', None)
        if activity_id is None:
          raise RuntimeError(f'An AWS stepfunctions activity name or ARN is required; either provide with --activity-id or set environment variable AWS_STEP_ACTIVITY_ID')
      self._activity_id = activity_id
    return self._activity_id

  def get_state_machine_id(self) -> str:
    if self._state_machine_id is None:
      state_machine_id: Optional[str] = self._args.state_machine_id
      if state_machine_id is None:
        state_machine_id = self._args.pre_state_machine_id
        if state_machine_id is None:
          state_machine_id = os.environ.get('AWS_STEP_STATE_MACHINE_ID', None)
          if state_machine_id is None:
            raise RuntimeError(f'An AWS stepfunctions state machine name or ARN is required; either provide with --state-machine-id or set environment variable AWS_STEP_STATE_MACHINE_ID')
      self._state_machine_id = state_machine_id
    return self._state_machine_id

  def get_state_machine_name(self) -> str:
    state_machine_name = self.get_state_machine_id()
    if ':' in state_machine_name:
      state_machine_name = get_aws_step_state_machine_name_from_arn(state_machine_name)
    return state_machine_name

  def get_s3_base_url(self) -> Optional[str]:
    if not self._have_s3_base_url:
      s3_base_url: Optional[str] = getattr(self._args, 's3_base_url', None)
      if s3_base_url is None:
        s3_base_url = getattr(self._args, 'pre_s3_base_url', None)
        if s3_base_url is None:
          s3_base_url = os.environ.get('AWS_STEP_S3_BASE_URL', None)
      self._s3_base_url = s3_base_url
      self._have_s3_base_url = True

    return self._s3_base_url

  def get_state_machine_base_url(self) -> Optional[str]:
    result = self.get_s3_base_url()
    if not result is None:
      if not result.endswith('/'):
        result += '/'
      result += self.get_state_machine_name()
    return result

  def get_execution_s3_base_url(self, execution_name: str) -> Optional[str]:
    result = self.get_state_machine_base_url()
    if not result is None:
      if not result.endswith('/'):
        result += '/'
      result += execution_name
    return result

  def require_execution_s3_base_url(self, execution_name: str) -> str:
    execution_url = self.get_execution_s3_base_url(execution_name)
    if execution_url is None:
      raise RuntimeError(f'--s3-base-url must be provided to determine URL of execution data')
    return execution_url

  def get_state_machine(self) -> AwsStepStateMachine:
    if self._state_machine is None:
      self._state_machine = AwsStepStateMachine(self.get_state_machine_id(), session=self.get_aws_session())
    return self._state_machine

  def pretty_print(
        self,
        value: Jsonable,
        compact: Optional[bool]=None,
        colorize: Optional[bool]=None,
        raw: Optional[bool]=None,
      ):

    if raw is None:
      raw = self._raw
    if raw:
      if isinstance(value, str):
        self._raw_stdout.write(value)
        return

    if compact is None:
      compact = self._compact
    if colorize is None:
      colorize = True

    def emit_to(f: TextIO):
      final_colorize = colorize and ((f is sys.stdout and self._colorize_stdout) or (f is sys.stderr and self._colorize_stderr))

      if not final_colorize:
        if compact:
          json.dump(value, f, separators=(',', ':'), sort_keys=True)
        else:
          json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n')
      else:
        jq_input = json.dumps(value, separators=(',', ':'), sort_keys=True)
        cmd = [ 'jq' ]
        if compact:
          cmd.append('-c')
        cmd.append('.')
        with subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=f) as proc:
          proc.communicate(input=jq_input.encode('utf-8'))
          exit_code = proc.returncode
        if exit_code != 0:
          raise subprocess.CalledProcessError(exit_code, cmd)

    output_file = self._output_file
    if output_file is None:
      emit_to(sys.stdout)
    else:
      with open(output_file, "w", encoding=self._encoding) as f:
        emit_to(f)

  def cmd_bare(self) -> int:
    print("A command is required", file=sys.stderr)
    return 1

  def cmd_version(self) -> int:
    self.pretty_print(pkg_version)
    return 0

  def cmd_test(self) -> int:
    args = self._args

    print(f"Test command, args={vars(args)}")

    return 0

  def cmd_list_activities(self) -> int:
    sfn = self.get_sfn()
    paginator = sfn.get_paginator('list_activities')
    page_iterator = paginator.paginate()
    result: JsonableList = []
    for page in page_iterator:
      for act_desc in page['activities']:
        result.append(dict(
            name=act_desc['name'],
            activityArn=act_desc['activityArn'],
            creationDate=str(act_desc['creationDate'])
          ))
    self.pretty_print(result)
    return 0

  def cmd_describe_activity(self) -> int:
    activity_id = self.get_activity_id()
    sfn = self.get_sfn()
    resp = describe_aws_step_activity(sfn, activity_id)
    self.pretty_print(resp)
    return 0

  def cmd_run(self) -> int:
    args = self._args
    max_task_total_seconds: Optional[float] = args.max_task_total_seconds
    if not max_task_total_seconds is None:
      max_task_total_seconds = float(max_task_total_seconds)
      if max_task_total_seconds <= 0.0:
        max_task_total_seconds = None
    worker = AwsStepActivityWorker(
        self.get_activity_id(),
        session=self.get_aws_session(),
        worker_name=args.worker_name,
        heartbeat_seconds=args.heartbeat_seconds,
        max_task_total_seconds=max_task_total_seconds,
        default_task_handler_class=args.default_task_handler_class
      )
    worker.run()
    return 0

  def cmd_create_chooser(self) -> int:
    args = self._args
    state_machine_id = self.get_state_machine_id()
    heartbeat_seconds: Optional[float] = args.heartbeat_seconds
    if heartbeat_seconds == 0.0:
      heartbeat_seconds = None
    timeout_seconds: Optional[float] = args.timeout_seconds
    if timeout_seconds == 0.0:
      timeout_seconds = None
    activity_timeout_seconds: Optional[float] = args.activity_timeout_seconds
    if activity_timeout_seconds is None:
      activity_timeout_seconds = timeout_seconds
    elif activity_timeout_seconds == 0.0:
      activity_timeout_seconds = None
    default_activity_id: Optional[str] = args.default_activity_id
    activity_ids: List[str] = args.choice_activity_ids
    state_machine = AwsStepStateMachine.create_with_activity_choices(
        state_machine_id,
        activity_ids=activity_ids,
        default_activity_id=default_activity_id,
        timeout_seconds=timeout_seconds,
        activity_heartbeat_seconds=heartbeat_seconds,
        activity_timeout_seconds=activity_timeout_seconds,
        session = self.get_aws_session()
      )
    self._state_machine = state_machine
    self.pretty_print(dict(arn=state_machine.state_machine_arn))
    return 0

  def cmd_describe_activity_chooser(self) -> int:
    args = self._args
    state_machine = self.get_state_machine()
    activity_names, default_activity_name = state_machine.get_activity_choices()
    result: JsonableDict = dict(
        activity_names=activity_names,
        param_name='activity',
        state_machine_arn=state_machine.state_machine_arn,
        state_machine_name=state_machine.state_machine_name,
        choice_state='SelectActivity',
        )
    if not default_activity_name is None:
      result['default_activity_name'] = default_activity_name
    self.pretty_print(result)
    return 0

  def cmd_update_chooser(self) -> int:
    args = self._args
    default_activity_id: Optional[str] = args.default_activity_id
    activity_ids: List[str] = args.choice_activity_ids
    heartbeat_seconds: Optional[float] = args.heartbeat_seconds
    if heartbeat_seconds == 0.0:
      heartbeat_seconds = None
    timeout_seconds: Optional[float] = args.timeout_seconds
    if timeout_seconds == 0.0:
      timeout_seconds = None
    state_machine = self.get_state_machine()
    state_machine.set_activity_choices(
        activity_ids=activity_ids,
        default_activity_id=default_activity_id,
        heartbeat_seconds=heartbeat_seconds,
        timeout_seconds=timeout_seconds,
      )
    state_machine.flush()
    return 0

  def cmd_add_activity_choice(self) -> int:
    args = self._args
    activity_id: str = args.choice_activity_id
    is_default: bool = args.is_default_activity_id
    heartbeat_seconds: Optional[float] = args.heartbeat_seconds
    if heartbeat_seconds == 0.0:
      heartbeat_seconds = None
    timeout_seconds: Optional[float] = args.timeout_seconds
    if timeout_seconds == 0.0:
      timeout_seconds = None
    state_machine = self.get_state_machine()
    state_machine.add_activity_choice(
        activity_id,
        is_default=is_default,
        heartbeat_seconds=heartbeat_seconds,
        timeout_seconds=timeout_seconds,
      )
    state_machine.flush()
    return 0

  def cmd_del_activity_choice(self) -> int:
    args = self._args
    activity_id: str = args.choice_activity_id
    state_machine = self.get_state_machine()
    state_machine.del_activity_choice(activity_id)
    state_machine.flush()
    return 0

  def gen_execution_name(self) -> str:
    return AwsStepStateMachine.gen_execution_name()

  def cmd_gen_execution_name(self) -> int:
    result = self.gen_execution_name()
    self.pretty_print(result)
    return 0

  def cmd_start_execution(self) -> int:
    args = self._args
    data_str: Optional[str] = args.data
    execution_name: Optional[str] = args.execution_name
    if execution_name is None:
      execution_name = self.gen_execution_name()
    data: JsonableDict
    if data_str is None:
      data = {}
    else:
      if data_str.startswith('@'):
        with open(data_str[1:], 'r') as fd:
          data_str = fd.read()
      data = json.loads(data_str)
    param_assignments: List[str] = args.param_value
    for param_assignment in param_assignments:
      if not '=' in param_assignment:
        raise ValueError(f'Parameter assignment requires "=": "{param_assignment}"')
      param_dest, param_str = param_assignment.split('=', 1)
      param_is_file = param_dest.endswith('@')
      if param_is_file:
        param_dest = param_dest[:-1]
        with open(param_str, 'r') as fd:
          param_str = fd.read()
      if ':' in param_dest:
        param_dest, param_type = param_dest.split(':', 1)
        param_type = param_type.lower()
      else:
        param_type = 'str'
      param_value: Jsonable
      if param_type == 'str':
        param_value = param_str
      elif param_type == 'json':
        param_value = json.loads(param_str)
      else:
        raise ValueError(f'Parameter assignment type must be "str" or "json": "{param_assignment}"')
      data[param_dest] = param_value

    if not 'execution_name' in data:
      data['execution_name'] = execution_name

    state_machine = self.get_state_machine()

    input_dir: Optional[str] = args.input_dir
    if not input_dir is None:
      inputs_url: str
      if 's3_inputs' in data:
        inputs_url = data['s3_inputs']
      else:
        execution_url = self.require_execution_s3_base_url(execution_name)
        inputs_url = execution_url + '/inputs'
        data['s3_inputs'] = inputs_url
      s3_upload_folder(inputs_url, input_dir, s3=self.get_s3(), session=self.get_aws_session())
    if not 's3_outputs' in data:
        optional_execution_url = self.get_execution_s3_base_url(execution_name)
        if not optional_execution_url is None:
          outputs_url = execution_url + '/outputs'
          data['s3_outputs'] = outputs_url
    result = state_machine.start_execution(name=execution_name, input_data=data)
    self.pretty_print(result)
    return 0

  def cmd_list_executions(self) -> int:
    args = self._args
    max_results: int = args.max_results
    status_filter: Optional[str] = args.status_filter
    next_token: Optional[str] = args.next_token
    state_machine = self.get_state_machine()
    result = state_machine.list_some_executions(max_results=max_results, next_token=next_token, status_filter=status_filter)
    self.pretty_print(result)
    return 0

  def cmd_describe_execution(self) -> int:
    args = self._args
    execution_name: str = args.execution_name

    state_machine = self.get_state_machine()
    result = state_machine.describe_execution(execution_name)
    self.pretty_print(result)
    return 0

  def cmd_wait_for_execution(self) -> int:
    args = self._args
    polling_interval_seconds: int = args.polling_interval_seconds
    execution_name: str = args.execution_name

    state_machine = self.get_state_machine()
    result = state_machine.wait_for_execution(execution_name, polling_interval_seconds=polling_interval_seconds, max_wait_seconds=None)
    self.pretty_print(result)
    return 0

  def download_execution_output_file_to_fileobj(
        self,
        execution_id: str,
        execution_output_filename: str,
        f: IO,
      ) -> None:
    state_machine = self.get_state_machine()
    state_machine.download_execution_output_file_to_fileobj(
        execution_id,
        execution_output_filename,
        f
      )

  def download_execution_output_file_to_stdout(
        self,
        execution_id: str,
        execution_output_filename: str,
      ) -> None:
    with os.fdopen(sys.stdout.fileno(), "wb", closefd=False) as f:
      self.download_execution_output_file_to_fileobj(execution_id, execution_output_filename, f)

  def download_execution_output_file_to_stderr(
        self,
        execution_id: str,
        execution_output_filename: str,
      ) -> None:
    with os.fdopen(sys.stderr.fileno(), "wb", closefd=False) as f:
      self.download_execution_output_file_to_fileobj(execution_id, execution_output_filename, f)

  def cmd_cat_execution_output(self) -> int:
    args = self._args
    execution_name: str = args.execution_name
    execution_output_filename: str = args.execution_output_file
    self.download_execution_output_file_to_stdout(execution_name, execution_output_filename)
    return 0


  def cmd_sign_s3_upload(self) -> int:
    args = self._args
    s3_url: str = args.s3_url
    expire_seconds = round(args.expire_seconds)
    result = generate_presigned_s3_upload_post(self.get_s3(), s3_url, expiration_seconds=expire_seconds)
    #curl_cmd = 'curl'
    #fields = result['fields']
    #post_url = result['url']
    #for k, v in sorted(fields.items()):
    #  curl_cmd += f" -F {k}='{v}'"
    #curl_cmd += f" -F file=@<local-file-path> '{post_url}'"
    #logger.info(f"To upload file with curl, use: {curl_cmd}")

    self.pretty_print(result)
    return 0

  def cmd_signed_upload(self) -> int:
    """CLI command to upload a file to S3 using a presigned post

    Example:
        aws-step-activity sign-s3-upload s3://amigos-dev-backend/testing/cli.py | \
            aws-step-activity signed-upload -p @/dev/stdin ./aws_step_activity/cli.py    

    Returns:
        int: CLI exit code. 0 on success.
    """
    args = self._args
    signed_post_str: str = args.signed_post.strip()
    filename: str = args.filename
    if signed_post_str.startswith('@'):
      with open(signed_post_str[1:], 'r') as fd:
        signed_post_str = fd.read()
    signed_post = json.loads(signed_post_str)
    upload_file_to_s3_with_signed_post(signed_post, filename)
    return 0

  def run(self) -> int:
    """Run the aws-step-activity commandline tool with provided arguments

    Args:
        argv (Optional[Sequence[str]], optional):
            A list of commandline arguments (NOT including the program as argv[0]!),
            or None to use sys.argv[1:]. Defaults to None.

    Returns:
        int: The exit code that would be returned if this were run as a standalone command.
    """
    parser = argparse.ArgumentParser(description="AWS step function activity tool.")

    # ======================= Main command

    self._parser = parser
    parser.add_argument('--traceback', "--tb", action='store_true', default=False,
                        help='Display detailed exception information')
    parser.add_argument('--loglevel', default='warning',
                        choices=['critical', 'error', 'warning', 'info', 'debug'],
                        help='Set the logging level. Default is "warning"')
    parser.add_argument('-M', '--monochrome', action='store_true', default=False,
                        help='Output to stdout/stderr in monochrome. Default is to colorize if stream is a compatible terminal')
    parser.add_argument('-c', '--compact', action='store_true', default=False,
                        help='Compact instead of pretty-printed output')
    parser.add_argument('-r', '--raw', action='store_true', default=False,
                        help='''Output raw strings and binary content directly, not json-encoded.
                                Values embedded in structured results are not affected.''')
    parser.add_argument('-o', '--output', dest="output_file", default=None,
                        help='Write output value to the specified file instead of stdout')
    parser.add_argument('--text-encoding', default='utf-8',
                        help='The encoding used for text. Default  is utf-8')
    parser.add_argument('-C', '--cwd', default='.',
                        help="Change the effective directory used to search for configuration")
    parser.add_argument('-p', '--aws-profile', default=None,
                        help='The AWS profile to use. Default is to use the default AWS settings')
    parser.add_argument('--aws-region', default=None,
                        help='The AWS region to use. Default is to use the default AWS region for the selected profile')
    parser.add_argument('-m', '--state-machine-id', dest='pre_state_machine_id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser.add_argument('-a', '--activity-id', default=None,
                        help='The AWS Step Function Activity name or Activity ARN. By default, environment variable AWS_STEP_ACTIVITY_ID is used.')
    parser.add_argument('-s', '--s3-base-url', dest='pre_s3_base_url', default=None,
                        help='The "s3://" URL that is the parent folder for all execution inputs/outputs. By default, environment variable AWS_STEP_S3_BASE_URL is used.')
    parser.set_defaults(func=self.cmd_bare)

    subparsers = parser.add_subparsers(
                        title='Commands',
                        description='Valid commands',
                        help='Additional help available with "aws-step-activity <command-name> -h"')

    # ======================= version

    parser_version = subparsers.add_parser('version',
                            description='''Display version information. JSON-quoted string. If a raw string is desired, use -r.''')
    parser_version.set_defaults(func=self.cmd_version)

    # ======================= list-activities

    parser_list_activities = subparsers.add_parser('list-activities',
                            description='''List the AWS stepfunction activities available in the AWS account/region.''')
    parser_list_activities.set_defaults(func=self.cmd_list_activities)

    # ======================= describe-activity

    parser_describe_activity = subparsers.add_parser('describe-activity',
                            description='''Describe the AWS stepfunction activity that will be serviced at runtime.''')
    parser_describe_activity.set_defaults(func=self.cmd_describe_activity)

    # ======================= create-activity-chooser

    parser_create_chooser = subparsers.add_parser('create-activity-chooser',
                            description='''Creates a simple AWS stepfunction state machine that chooses between a list of activities.''')
    parser_create_chooser.add_argument('--heartbeat-seconds', type=float, default=60.0,
                        help='The required interval for receiving heartbeats, in seconds. If 0, heartbeats wil not be required. By default, 60 seconds is used.')
    parser_create_chooser.add_argument('--timeout-seconds', type=float, default=600.0,
                        help='The default maximum execution runtime to entire executions, in seconds. If 0, no limit will be imposed. By default, a 10-minute limit is imposed.')
    parser_create_chooser.add_argument('--activity-timeout-seconds', type=float, default=600.0,
                        help='The default maximum execution runtime for each activity, in seconds. If 0, no limit will be imposed. By default, the total execution limit is imposed.')
    parser_create_chooser.add_argument('--default-activity-id', default=None,
                        help='The default chosen activity if none is selected in a job. By default, an error will result if none is chosen.')
    parser_create_chooser.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_create_chooser.add_argument('choice_activity_ids', nargs=argparse.REMAINDER, default=[],
                        help='A list of activity names or ARNs that consitute named choices for the state machine.')
    parser_create_chooser.set_defaults(func=self.cmd_create_chooser)

    # ======================= describe-activity-chooser

    parser_describe_activity_chooser = subparsers.add_parser('describe-activity-chooser',
                            description='''Displays choices for a simple AWS stepfunction state machine that chooses between a list of activities.''')
    parser_describe_activity_chooser.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_describe_activity_chooser.set_defaults(func=self.cmd_describe_activity_chooser)

    # ======================= update-activity-chooser

    parser_update_chooser = subparsers.add_parser('update-activity-chooser',
                            description='''Update the activity choiices for a simple AWS stepfunction state machine that chooses between a list of activities.''')
    parser_update_chooser.add_argument('--default-activity-id', default=None,
                        help='The default chosen activity if none is selected in a job. By default, an error will result if none is chosen.')
    parser_update_chooser.add_argument('--heartbeat-seconds', type=float, default=60.0,
                        help='For newly created states, the required interval for receiving heartbeats, in seconds. If 0, heartbeats wil not be required. By default, 60 seconds is used.')
    parser_update_chooser.add_argument('--timeout-seconds', type=float, default=600.0,
                        help='For newly create states, the default maximum execution runtime, in seconds. If 0, no limit will be imposed. By default, a 10-minute limit is imposed.')
    parser_update_chooser.add_argument('choice_activity_ids', nargs=argparse.REMAINDER, default=[],
                        help='A list of activity names or ARNs that consitute named choices for the state machine.')
    parser_update_chooser.set_defaults(func=self.cmd_update_chooser)

    # ======================= add-activity-choice

    parser_add_activity_choice = subparsers.add_parser('add-activity-choice',
                            description='''Adds an activity choice to a simple AWS stepfunction state machine, and optionally sets it as the default choice.''')
    parser_add_activity_choice.add_argument('--default', dest='is_default_activity_id', action='store_true', default=False,
                            help='If provided, the activity will be the default activity. By default, the activity will not be used as the default activity.')
    parser_add_activity_choice.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_add_activity_choice.add_argument('--heartbeat-seconds', type=float, default=60.0,
                        help='For newly created states, the required interval for receiving heartbeats, in seconds. If 0, heartbeats wil not be required. By default, 60 seconds is used.')
    parser_add_activity_choice.add_argument('--timeout-seconds', type=float, default=600.0,
                        help='For newly create states, the default maximum execution runtime, in seconds. If 0, no limit will be imposed. By default, a 10-minute limit is imposed.')
    parser_add_activity_choice.add_argument('choice_activity_id',
                        help='The activity name or ARN of the activity to be added as a choice. The activity will be created if it does not exist.')
    parser_add_activity_choice.set_defaults(func=self.cmd_add_activity_choice)

    # ======================= del-activity-choice

    parser_del_activity_choice = subparsers.add_parser('del-activity-choice',
                            description='''Deletes an activity choice from a simple AWS stepfunction state machine.''')
    parser_del_activity_choice.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_del_activity_choice.add_argument('choice_activity_id',
                        help='The activity name or ARN of the activity to be deleted as a choice.')
    parser_del_activity_choice.set_defaults(func=self.cmd_del_activity_choice)

    # ======================= gen-execution-name

    parser_gen_execution_name = subparsers.add_parser('gen-execution-name',
                            description='''Generates a new unique execution name.''')
    parser_gen_execution_name.set_defaults(func=self.cmd_gen_execution_name)

    # ======================= start-execution

    parser_start_execution = subparsers.add_parser('start-execution',
                            description='''Starts a new execution of an AWS stepfunction state machine.''')
    parser_start_execution.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_start_execution.add_argument('-n', '--name', dest='execution_name', default=None,
                        help='The name of the execution. By default, a new guid is used.')
    parser_start_execution.add_argument('-d', '--data', default=None,
                        help='The base input data, JSON dict string, to pass to the new execution. If "@<filename>" is '
                             'provided, the data is read from the specified file. This data can be further modified with'
                             '-s, -p, -i, and -o options. By default, "{}" is used as data.')
    parser_start_execution.add_argument('-v', '--param-value', default=[], action='append',
                        help='A string in the form "<param-name>[:<param-type>][@]=<param-value>". The named parameter is '
                             'added to the base input data. This option may be repeated. <param-type> may be json or str; by default str '
                             'is assumed. If "@" is appended to the param name, then <param-value> is interpreted as a file from which '
                             'the actual value is read. By default, no parameters are added to the base input data.')
    parser_start_execution.add_argument('-s', '--s3-base-url', default=None,
                        help='The "s3://" URL that is the parent folder for all execution inputs/outputs. By default, environment variable AWS_STEP_S3_BASE_URL is used.')
    parser_start_execution.add_argument('-i', '--input-dir', default=None,
                        help='The local inputs data folder that should be uploaded to S3 before execution. By default, no folder is uploaded.')
    parser_start_execution.set_defaults(func=self.cmd_start_execution)

    # ======================= describe-execution

    parser_describe_execution = subparsers.add_parser('describe-execution',
                            description='''Gets the status of a specific execution of an AWS stepfunction state machine.''')
    parser_describe_execution.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_describe_execution.add_argument('execution_name',
                        help='The name of the execution.')
    parser_describe_execution.set_defaults(func=self.cmd_describe_execution)

    # ======================= wait-for-execution

    parser_wait_for_execution = subparsers.add_parser('wait-for-execution',
                            description='''Waits for an execution to complete.''')
    parser_wait_for_execution.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_wait_for_execution.add_argument('-p', '--polling-interval-seconds', type=int, default=10,
                        help='The interval at which to poll AWS for results, in seconds. By default, the execution is polled every 10 seconds')
    parser_wait_for_execution.add_argument('execution_name',
                        help='The name of the execution.')
    parser_wait_for_execution.set_defaults(func=self.cmd_wait_for_execution)

    # ======================= cat-execution-output

    parser_cat_execution_output = subparsers.add_parser('cat-execution-output',
                            description='''Copies the contents of an exetion outputfile to stdout.''')
    parser_cat_execution_output.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_cat_execution_output.add_argument('execution_name',
                        help='The name of the execution.')
    parser_cat_execution_output.add_argument('execution_output_file',
                        help='The name of the execution output file, relative to the execution\'s S3 output folder.')
    parser_cat_execution_output.set_defaults(func=self.cmd_cat_execution_output)

    # ======================= list-executions

    parser_list_executions = subparsers.add_parser('list-executions',
                            description='''Lists a subset of the executions associated with the state machine.''')
    parser_list_executions.add_argument('-m', '--state-machine-id', default=None,
                        help='The AWS Step Function state machine name or state machine ARN. By default, environment variable AWS_STEP_STATE_MACHINE is used.')
    parser_list_executions.add_argument('-n', '--max-results', type=int, default=1000,
                        help='The maximum number of results to return. Must be <= 1000. By default, 1000 is used.')
    parser_list_executions.add_argument('-s', '--status', dest='status_filter', default=None,
                        help='The a filter for matching status values. By default, all executions are returned regardless of status.')
    parser_list_executions.add_argument('-t', '--next-token', default=None,
                        help='The nextToken value from the previous invocation, to continue listing. By default, starts at the beginning.')
    parser_list_executions.set_defaults(func=self.cmd_list_executions)


    # ======================= sign-s3-upload

    parser_sign_s3_upload = subparsers.add_parser('sign-s3-upload',
                            description='''Signs a POST request for upload to S3.''')
    parser_sign_s3_upload.add_argument('--expire-seconds', type=float, default=600.0,
                        help='The maximum number of seconds before the signed URL must be used. By default, 10 minutes is the limit.')
    parser_sign_s3_upload.add_argument('s3_url',
                        help='The s3:// url of the object to be uploaded.')
    parser_sign_s3_upload.set_defaults(func=self.cmd_sign_s3_upload)

    # ======================= signed-upload

    parser_signed_upload = subparsers.add_parser('signed-upload',
                            description='''Uploads a file to S3 using a presigned POST.''')
    parser_signed_upload.add_argument('-p', '--signed-post',
                        help='The presigned POST metadata (as produced by sign-s3-upload), as JSON, or "@" followed by a filename containing the JSON')
    parser_signed_upload.add_argument('filename',
                        help='The local filename containing the content to be uploaded.')
    parser_signed_upload.set_defaults(func=self.cmd_signed_upload)

    # ======================= run

    parser_run = subparsers.add_parser('run',
                            description='''Run an AWS step activity worker.''')
    parser_run.add_argument('-w', '--worker-name', default=None,
                        help='The worker name, used for logging and completion rep[orting]. By default, a unique ID mased on local MAC address is used.')
    parser_run.add_argument('--heartbeat-seconds', type=float, default=20.0,
                        help='The default interval for sending heartbeats, in seconds. Overridden by task definition. By default, 20.0 seconds is used.')
    parser_run.add_argument('--max-task-total-seconds', type=float, default=None,
                        help='The default maximum task runtime, in seconds. Overridden by task definition. By default, No limit is imposed.')
    parser_run.add_argument('--default-task-handler-class', default=None,
                        help='The default fully qualified task handler Python class name. Overridden by task definition. By default, a simple commandline task handler is used.')
    parser_run.set_defaults(func=self.cmd_run)

    # ======================= test

    parser_test = subparsers.add_parser('test', description="Run a simple test. For debugging only.  Will be removed.")
    parser_test.set_defaults(func=self.cmd_test)

    # =========================================================

    #argcomplete.autocomplete(parser)
    try:
      args = parser.parse_args(self._argv)
    except ArgparseExitError as ex:
      return ex.exit_code
    logging.basicConfig(level=args.loglevel.upper())
    logLevel = logging.getLogger().level
    # Restrict loglevel of boto3 and urllib3 modules because they are very chatty
    # and it is hard to find our log messages amongst the noise
    for modname in [
      'botocore.hooks','botocore.parsers','botocore.auth','botocore.endpoint','botocore.httpsession',
      'botocore.loaders','botocore.retryhandler','botocore.utils','botocore.client',
      'botocore.session','botocore.handlers','botocore.awsrequest','botocore.regions','urllib3.connectionpool',
      's3transfer.utils','s3transfer.tasks','s3transfer.futures']:
      logging.getLogger(modname).setLevel(max(logLevel, logging.INFO))
    logging.getLogger('botocore.credentials').setLevel(max(logLevel, logging.WARNING))
    traceback: bool = args.traceback
    try:
      self._args = args
      self._raw_stdout = sys.stdout
      self._raw_stderr = sys.stderr
      self._raw = args.raw
      self._compact = args.compact
      self._output_file = args.output_file
      self._encoding = args.text_encoding
      monochrome: bool = args.monochrome
      if not monochrome:
        self._colorize_stdout = is_colorizable(sys.stdout)
        self._colorize_stderr = is_colorizable(sys.stderr)
        if self._colorize_stdout or self._colorize_stderr:
          colorama.init(wrap=False)
          if self._colorize_stdout:
            new_stream = colorama.AnsiToWin32(sys.stdout)
            if new_stream.should_wrap():
              sys.stdout = new_stream
          if self._colorize_stderr:
            new_stream = colorama.AnsiToWin32(sys.stderr)
            if new_stream.should_wrap():
              sys.stderr = new_stream
      self._cwd = os.path.abspath(os.path.expanduser(args.cwd))
      rc = args.func()
    except Exception as ex:
      if isinstance(ex, CmdExitError):
        rc = ex.exit_code
      else:
        rc = 1
      if rc != 0:
        if traceback:
          raise

        print(f"{self.ecolor(Fore.RED)}aws-step-activity: error: {ex}{self.ecolor(Style.RESET_ALL)}", file=sys.stderr)
    return rc

  @property
  def args(self) -> argparse.Namespace:
    return self._args

def run(argv: Optional[Sequence[str]]=None) -> int:
  try:
    rc = CommandLineInterface(argv).run()
  except CmdExitError as ex:
    rc = ex.exit_code
  return rc

class CommandHandler:
  cli: CommandLineInterface
  args: argparse.Namespace

  def __init__(self, cli: CommandLineInterface):
    self.cli = cli
    self.args = cli.args

  def __call__(self) -> int:
    raise NotImplementedError(f"{full_type(self)} has not implemented __call__")
