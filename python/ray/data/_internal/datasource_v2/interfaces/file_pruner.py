from abc import ABC, abstractmethod


class FilePruner(ABC):
    """Generic file-level filter applied during listing."""

    @abstractmethod
    def should_include(self, path: str) -> bool:
        """Return True if this file should be included, False to skip it."""
        ...
