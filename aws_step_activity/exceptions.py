#
# Copyright (c) 2022 Amigos development, Inc.
#
# MIT License - See LICENSE file accompanying this package.
#

"""Exceptions defined by this package"""

from typing import Optional

from .internal_types import JsonableDict

class AwsStepActivityError(Exception):
  """Base class for all error exceptions defined by this package."""
  #pass
