from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SaveMode(str, Enum):
    """Enum of possible modes for saving/writing data."""

    APPEND = "append"
    """Add new data without modifying existing data."""

    OVERWRITE = "overwrite"
    """Replace all existing data with new data."""

    IGNORE = "ignore"
    """Don't write if data already exists."""

    ERROR = "error"
    """Raise an error if data already exists."""

    UPSERT = "upsert"
    """Update existing rows that match on key fields, or insert new rows.
    Requires identifier/key fields to be specified.
    """
