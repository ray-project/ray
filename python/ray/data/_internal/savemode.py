from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SaveMode(str, Enum):
    """Enum of possible modes for saving/writing data.

    Attributes:
        APPEND: Add new data without modifying existing data.
        OVERWRITE: Replace all existing data with new data.
        IGNORE: Don't write if data already exists.
        ERROR: Raise an error if data already exists.
        UPSERT: Update existing rows that match on key fields, or insert new rows.
            Requires identifier/key fields to be specified.
    """

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
    UPSERT = "upsert"
