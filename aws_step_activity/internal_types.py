#
# Copyright (c) 2022 Samuel J. McKelvie
#
# MIT License - See LICENSE file accompanying this package.
#

"""Type hints used internally by this package"""

from typing import (
    Dict,
    Union,
    Any,
    List,
    Optional,
    Callable,
    Awaitable,
    NewType,
    AsyncIterable,
    AsyncGenerator,
    AsyncContextManager,
    AsyncIterable,
    Tuple,
    Type,
    Set,
    TypeVar,
    TYPE_CHECKING,
    FrozenSet,
    Coroutine,
    Generator,
    Iterable,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
  )

from botocore.exceptions import ClientError
from botocore.client import BaseClient

# Only use mypy during type-checking (these libraries are not included in
# non-development install)
if TYPE_CHECKING:
  from mypy_boto3_stepfunctions.client import SFNClient
  from mypy_boto3_s3.client import S3Client
  from mypy_boto3_logs.client import CloudWatchLogsClient
  from mypy_boto3_iam.client import IAMClient
  from mypy_boto3_sts.client import STSClient
  from mypy_boto3_s3.type_defs import ObjectTypeDef as S3_ObjectTypeDef
  from mypy_boto3_stepfunctions.type_defs import (
      LoggingConfigurationTypeDef as SFN_LoggingConfigurationTypeDef,
      TracingConfigurationTypeDef as SFN_TracingConfigurationTypeDef
    )
else:
  SFNClient = BaseClient
  S3Client = BaseClient
  CloudWatchLogsClient = BaseClient
  IAMClient = BaseClient
  STSClient = BaseClient
  S3_ObjectTypeDef = Dict[str, Any]
  SFN_LoggingConfigurationTypeDef = Dict[str, Any]
  SFN_TracingConfigurationTypeDef = Dict[str, Any]

JsonableTypes = ( str, int, float, bool, dict, list )
# A tuple of types to use for isinstance checking of JSON-serializable types. Excludes None. Useful for isinstance.

if TYPE_CHECKING:
  Jsonable = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]
  """A Type hint for a simple JSON-serializable value; i.e., str, int, float, bool, None, Dict[str, Jsonable], List[Jsonable]"""
else:
  Jsonable = Union[str, int, float, bool, None, Dict[str, 'Jsonable'], List['Jsonable']]
  """A Type hint for a simple JSON-serializable value; i.e., str, int, float, bool, None, Dict[str, Jsonable], List[Jsonable]"""

JsonableDict = Dict[str, Jsonable]
"""A type hint for a simple JSON-serializable dict; i.e., Dict[str, Jsonable]"""

JsonableList = List[Jsonable]
"""A type hint for a simple JSON-serializable list; i.e., Stist[Jsonable]"""
