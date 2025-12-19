from enum import Enum

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SaveMode(str, Enum):
    """Enum of possible modes for saving/writing data."""

    """Add new data without modifying existing data."""
    APPEND = "append"

    """Replace all existing data with new data."""
    OVERWRITE = "overwrite"

    """Don't write if data already exists."""
    IGNORE = "ignore"

    """Raise an error if data already exists."""
    ERROR = "error"

    """Update existing rows that match on key fields, or insert new rows.
    Requires identifier/key fields to be specified.
    """
    UPSERT = "upsert"
