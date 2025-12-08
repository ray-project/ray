from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SaveMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
