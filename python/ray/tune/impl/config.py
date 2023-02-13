import abc


class _Config(abc.ABC):
    def to_dict(self) -> dict:
        """Converts this configuration to a dict format."""
        raise NotImplementedError
