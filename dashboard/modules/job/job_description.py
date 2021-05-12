from enum import Enum
from typing import Dict, Any
from pydantic import BaseModel as PydanticBaseModel, Extra
from ray.core.generated import common_pb2


class BaseModel(PydanticBaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid


class Language(str, Enum):
    PYTHON = common_pb2.Language.Name(common_pb2.PYTHON)
    JAVA = common_pb2.Language.Name(common_pb2.JAVA)
    CPP = common_pb2.Language.Name(common_pb2.CPP)


class RuntimeEnv(BaseModel):
    # The url to download the job package archive. The archive format is
    # one of “zip”, “tar”, “gztar”, “bztar”, or “xztar”. Please refer to
    # https://docs.python.org/3/library/shutil.html#shutil.unpack_archive
    working_dir: str


class JobDescription(BaseModel):
    # The job driver language, this field determines how to start the
    # driver. The value is one of the names of enum Language defined in
    # common.proto, e.g. PYTHON
    language: Language
    # The runtime_env (RuntimeEnvDict) for the job config.
    runtime_env: RuntimeEnv
    # The entry to start the driver.
    # PYTHON:
    #   - The basename of driver filename without extension in the job
    #   package archive.
    # JAVA:
    #   - The driver class full name in the job package archive.
    driver_entry: str
    # The driver arguments in list.
    # PYTHON:
    #   -  The arguments to pass to the main() function in driver entry.
    #   e.g. [1, False, 3.14, "abc"]
    # JAVA:
    #   - The arguments to pass to the driver command line.
    #   e.g. ["-custom-arg", "abc"]
    driver_args: list = []
    # The environment vars to pass to job config, type of keys should be str.
    env: Dict[str, Any] = {}
