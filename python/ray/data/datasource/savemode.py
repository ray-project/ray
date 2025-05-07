from enum import Enum


class SaveMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
