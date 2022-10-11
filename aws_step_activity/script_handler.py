# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""AWS step function activity task handler that runs scripting language scripts (e.g., bash or python)"""

from .logging import logger

from time import sleep
from typing import Optional, Dict, Any

from .internal_types import Jsonable, JsonableDict

import os
import subprocess

from .handler import AwsStepActivityTaskHandler

class AwsStepScriptHandler(AwsStepActivityTaskHandler):
  """An AWS stepfunction activity task handler that runs scripts
  """

  subproc: Optional[subprocess.Popen] = None

  def run_in_context(self) -> JsonableDict:
    data = self.input_data

    env = dict(os.environ)
    session = self.session
    if not session.profile_name is None:
      env['AWS_PROFILE'] = session.profile_name
    if not session.region_name is None:
      env['AWS_REGION'] = session.region_name
    if 'env' in data:
      env.update(data['env'])

    script_text = data.get('script', "echo 'Hello, world'\n")
    if not script_text.startswith('#!'):
      script_text = "#!/bin/bash\n" + script_text
    self.script_text = script_text

    cwd = self.task_working_dir
    input_dir = self.task_input_dir
    output_dir = self.task_output_dir
    script_file = os.path.join(input_dir, 'script_file')
    with open(script_file, 'w') as f:
      f.write(script_text)
    os.chmod(script_file, 0o700)
    stdout_file = os.path.join(output_dir, 'stdout.txt')
    stderr_file = os.path.join(output_dir, 'stderr.txt')
    exit_code: Optional[int] = None
    with open(stderr_file, 'wb') as f_stderr:
      with open(stdout_file, 'wb') as f_stdout:
        with subprocess.Popen(
              [ script_file ],
              stdout=f_stdout,
              stderr=f_stderr,
              stdin=subprocess.PIPE,
              cwd=self.task_working_dir,
              env=env
            ) as p:
          self.subproc = p
          try:
            p.communicate(input=b'')
          finally:
            self.subproc = None
          exit_code = p.returncode
          logger.debug(f"Script exited with exit code {exit_code}")
    if exit_code != 0:
      raise RuntimeError(f"Script exited with nonzero completion code {exit_code}")

  def on_complete_sent_locked(self):
    p = self.subproc
    if not p is None:
      logger.warn(f'on_complete_sent: terminating subprocess early')
      try:
        p.terminate()
      except Exception:
        pass
