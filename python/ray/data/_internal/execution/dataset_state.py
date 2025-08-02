import enum


class DatasetState(enum.IntEnum):
    """Enum representing the possible states of a dataset during execution."""

    UNKNOWN = 0
    RUNNING = 1
    FINISHED = 2
    FAILED = 3
    PENDING = 4

    def __str__(self):
        return self.name

    @classmethod
    def from_string(cls, text):
        """Get enum by name."""
        try:
            return cls[text]  # This uses the name to lookup the enum
        except KeyError:
            return cls.UNKNOWN
