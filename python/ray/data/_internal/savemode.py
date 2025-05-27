from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stablity="alpha")
class SaveMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
