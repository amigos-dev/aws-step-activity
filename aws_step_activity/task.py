# Copyright (c) 2022 Amigos Development Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""Implementation of AwsStepActivityTask"""

from .logging import logger

from typing import TYPE_CHECKING
from .internal_types import Jsonable, JsonableDict

import json

class AwsStepActivityTask:
  """A descriptor for a single task in an AWS Step Function Activity"""

  task_token: str
  """The task token, passed to AWS stepfunction APIs that manipulate the task"""

  data: Jsonable
  """The input data for the task, in deserialized JSON form"""

  def __init__(self, resp: JsonableDict):
    """Create an AWS stepfunction activity task descriptor

    Args:
        resp (JsonableDict): The deserialized JSON response object from SFN.get_activity_task()

    Raises:
        RuntimeError: The resp object is poorly formed
    """
    if (
          not isinstance(resp, dict) or 
          not 'taskToken' in resp or
          not isinstance(resp['taskToken'], str) or 
          resp['taskToken'] == '' or
          not 'input' in resp or 
          not isinstance(resp['input'], str)
        ):
      raise RuntimeError("Invalid AWS stepfunctions task descriptor")
    self.task_token = resp['taskToken']
    try:
      self.data = json.loads(resp['input'])
    except Exception as ex:
      raise RuntimeError("Invalid AWS stepfunctions task descriptor--invalid JSON input") from ex
