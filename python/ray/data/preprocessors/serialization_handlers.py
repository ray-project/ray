"""
Serialization handlers for preprocessor save/load functionality.

This module implements a factory pattern to abstract different serialization formats,
making it easier to add new formats and maintain existing ones.
"""

import abc
import base64
import pickle
from enum import Enum
from typing import Any, Dict, Optional, Union

from ray.cloudpickle import cloudpickle
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class HandlerFormatName(Enum):
    """Enum for consistent format naming in the factory."""

    CLOUDPICKLE = "cloudpickle"
    PICKLE = "pickle"


@DeveloperAPI
class SerializationHandler(abc.ABC):
    """Abstract base class for handling preprocessor serialization formats."""

    @abc.abstractmethod
    def serialize(
        self, data: Union["Preprocessor", Dict[str, Any]]  # noqa: F821
    ) -> Union[str, bytes]:
        """Serialize preprocessor data to the specific format.

        Args:
            data: Dictionary containing preprocessor metadata and stats

        Returns:
            Serialized data in format-specific representation
        """
        pass

    @abc.abstractmethod
    def deserialize(self, serialized: Union[str, bytes]) -> Any:
        """Deserialize data from the specific format.

        Args:
            serialized: Serialized data in format-specific representation

        Returns:
            For structured formats (CloudPickle/JSON/MessagePack): Dictionary containing preprocessor metadata and stats
            For pickle format: The actual deserialized object
        """
        pass

    @abc.abstractmethod
    def get_magic_bytes(self) -> Union[str, bytes]:
        """Get the magic bytes/prefix for this format."""
        pass

    def strip_magic_bytes(self, serialized: Union[str, bytes]) -> Union[str, bytes]:
        """Remove magic bytes from serialized data."""
        magic = self.get_magic_bytes()
        if isinstance(serialized, (str, bytes)) and serialized.startswith(magic):
            return serialized[len(magic) :]
        return serialized


@DeveloperAPI
class CloudPickleSerializationHandler(SerializationHandler):
    """Handler for CloudPickle serialization format."""

    MAGIC_CLOUDPICKLE = b"CPKL:"

    def serialize(
        self, data: Union["Preprocessor", Dict[str, Any]]  # noqa: F821
    ) -> bytes:
        """Serialize to CloudPickle format with magic prefix."""
        return self.MAGIC_CLOUDPICKLE + cloudpickle.dumps(data)

    def deserialize(self, serialized: bytes) -> Dict[str, Any]:
        """Deserialize from CloudPickle format."""
        if not isinstance(serialized, bytes):
            raise ValueError(
                f"Expected bytes for CloudPickle deserialization, got {type(serialized)}"
            )

        if not serialized.startswith(self.MAGIC_CLOUDPICKLE):
            raise ValueError(f"Invalid CloudPickle magic bytes: {serialized[:10]}")

        cloudpickle_data = self.strip_magic_bytes(serialized)
        return cloudpickle.loads(cloudpickle_data)

    def get_magic_bytes(self) -> bytes:
        return self.MAGIC_CLOUDPICKLE


@DeveloperAPI
class PickleSerializationHandler(SerializationHandler):
    """Handler for legacy Pickle serialization format."""

    def serialize(
        self, data: Union["Preprocessor", Dict[str, Any]]  # noqa: F821
    ) -> str:
        """
        Serialize using pickle format (for backward compatibility).
        data is ignored, but kept for consistency

        """
        return base64.b64encode(pickle.dumps(data)).decode("ascii")

    def deserialize(
        self, serialized: str
    ) -> Any:  # Returns the actual object, not metadata
        """Deserialize from pickle format (legacy support)."""
        # For pickle, we return the actual deserialized object directly
        return pickle.loads(base64.b64decode(serialized))

    def get_magic_bytes(self) -> str:
        return ""  # Pickle format doesn't use magic bytes


class SerializationHandlerFactory:
    """Factory class for creating appropriate serialization handlers."""

    _handlers = {
        HandlerFormatName.CLOUDPICKLE: CloudPickleSerializationHandler,
        HandlerFormatName.PICKLE: PickleSerializationHandler,
    }

    @classmethod
    def register_handler(cls, format_name: HandlerFormatName, handler_class: type):
        """Register a new serialization handler."""
        cls._handlers[format_name] = handler_class

    @classmethod
    def get_handler(
        cls,
        format_identifier: Optional[HandlerFormatName] = None,
        data: Optional[Union[str, bytes]] = None,
        **kwargs,
    ) -> SerializationHandler:
        """Get the appropriate serialization handler for a format or serialized data.

        Args:
            format_identifier: The format to use for serialization. If None, will detect from data.
            data: Serialized data to detect format from (used when format_identifier is None).
            **kwargs: Additional keyword arguments (currently unused).

        Returns:
            SerializationHandler instance for the format

        Raises:
            ValueError: If format is not supported or cannot be detected
        """
        # If it's already a format enum, use it directly
        if not format_identifier:
            format_identifier = cls.detect_format(data)

        if format_identifier not in cls._handlers:
            raise ValueError(
                f"Unsupported serialization format: {format_identifier.value}. "
                f"Supported formats: {list(cls._handlers.keys())}"
            )

        handler_class = cls._handlers[format_identifier]
        return handler_class()

    @classmethod
    def detect_format(cls, serialized: Union[str, bytes]) -> HandlerFormatName:
        """Detect the serialization format from the magic bytes.

        Args:
            serialized: Serialized data

        Returns:
            Format name enum

        Raises:
            ValueError: If format cannot be detected
        """
        # Check for CloudPickle first (binary format)
        if isinstance(serialized, bytes) and serialized.startswith(
            CloudPickleSerializationHandler.MAGIC_CLOUDPICKLE
        ):
            return HandlerFormatName.CLOUDPICKLE

        # Check for legacy pickle format (no magic bytes, should be base64 encoded)
        if isinstance(serialized, str):
            return HandlerFormatName.PICKLE

        raise ValueError(
            f"Cannot detect serialization format from: {serialized[:20]}..."
        )
