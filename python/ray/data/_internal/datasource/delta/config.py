"""
Configuration classes for Delta Lake datasource.
"""

from enum import Enum


class WriteMode(Enum):
    """Write modes for Delta Lake tables."""

    ERROR = "error"
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
