"""Unbound position management.

This module handles position tracking across different unbound sources.
"""

from datetime import datetime, timedelta
from typing import Union

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class UnboundPosition:
    """Represents a position in an unbound source."""

    def __init__(self, value: Union[str, int, datetime], position_type: str = "offset"):
        """Initialize an unbound position."""
        self.value = value
        self.position_type = position_type

        # Validate position type
        valid_types = {"offset", "timestamp", "sequence", "checkpoint", "custom"}
        if position_type not in valid_types:
            # Use a default type instead of logging
            self.position_type = "custom"

    def __eq__(self, other: object) -> bool:
        """Compare positions for equality."""
        if not isinstance(other, UnboundPosition):
            return False
        return self.position_type == other.position_type and self.value == other.value

    def __lt__(self, other: "UnboundPosition") -> bool:
        """Compare positions for ordering."""
        if self.position_type != other.position_type:
            raise ValueError("Cannot compare different position types")

        if isinstance(self.value, (int, float)) and isinstance(
            other.value, (int, float)
        ):
            return self.value < other.value
        elif isinstance(self.value, datetime) and isinstance(other.value, datetime):
            return self.value < other.value
        else:
            raise ValueError(
                f"Cannot compare position values of type {type(self.value)}"
            )

    def __str__(self) -> str:
        """String representation of the position."""
        return f"{self.position_type}:{self.value}"

    @classmethod
    def from_string(cls, position_str: str) -> "UnboundPosition":
        """Parse position from string representation."""
        if ":" in position_str:
            position_type, value = position_str.split(":", 1)
            return cls(value, position_type)
        else:
            return cls(position_str, "offset")

    def advance(self, increment: Union[int, float, timedelta]) -> "UnboundPosition":
        """Advance the position by the given increment."""
        if self.position_type == "offset" and isinstance(self.value, int):
            return UnboundPosition(self.value + increment, self.position_type)
        elif self.position_type == "timestamp" and isinstance(self.value, datetime):
            if isinstance(increment, timedelta):
                return UnboundPosition(self.value + increment, self.position_type)
            else:
                raise ValueError(
                    "Timestamp positions can only be advanced by timedelta"
                )
        elif self.position_type == "sequence" and isinstance(self.value, int):
            return UnboundPosition(self.value + increment, self.position_type)
        else:
            raise ValueError(f"Cannot advance position of type {self.position_type}")


def standardize_position(position: str, position_type: str = "offset") -> str:
    """Standardize position format across unbound sources."""
    if not position:
        return f"{position_type}:0"

    # If already in standardized format, return as-is
    if ":" in position and position.split(":", 1)[0] in [
        "offset",
        "sequence",
        "checkpoint",
        "timestamp",
    ]:
        return position

    # Otherwise, add the position type prefix
    return f"{position_type}:{position}"


def parse_position(position_str: str) -> tuple[str, str]:
    """Parse standardized position string."""
    if ":" in position_str:
        return position_str.split(":", 1)
    else:
        # Default to offset type for backwards compatibility
        return "offset", position_str
