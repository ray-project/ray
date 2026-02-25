class UnknownPreprocessorError(ValueError):
    """Raised when attempting to deserialize an unknown/unregistered preprocessor type."""

    def __init__(self, preprocessor_type: str):
        self.preprocessor_type = preprocessor_type
        super().__init__(f"Unknown preprocessor type: {preprocessor_type}")


_PREPROCESSOR_REGISTRY = {}


def SerializablePreprocessor(version: int, identifier: str):
    """Register a preprocessor class for serialization.

    This decorator registers a preprocessor class in the serialization registry,
    enabling it to be serialized and deserialized. The decorated class MUST inherit
    from SerializablePreprocessor.

    Args:
        version: Version number for this preprocessor's serialization format
        identifier: Stable identifier for serialization. This identifier will be used
            in serialized data. Using an explicit identifier allows classes to be
            renamed without breaking compatibility with existing serialized data.

    Returns:
        A decorator function that registers the class and returns it unchanged.

    Raises:
        TypeError: If the decorated class does not inherit from SerializablePreprocessor

    Note:
        If a class with the same identifier is already registered, logs an info message
        and overwrites the previous registration.

    Examples:
        @SerializablePreprocessor(version=1, identifier="my_preprocessor_v1")
        class MyPreprocessor(SerializablePreprocessor):
            pass
    """

    def decorator(cls):
        import logging

        from ray.data.preprocessor import SerializablePreprocessorBase

        # Verify that the class inherits from SerializablePreprocessor
        if not issubclass(cls, SerializablePreprocessorBase):
            raise TypeError(
                f"Class {cls.__module__}.{cls.__qualname__} must inherit from "
                f"SerializablePreprocessor to use @SerializablePreprocessor decorator."
            )

        cls.set_version(version)
        cls.set_preprocessor_class_id(identifier)

        # Check for collisions and log info message
        if identifier in _PREPROCESSOR_REGISTRY:
            existing = _PREPROCESSOR_REGISTRY[identifier]
            if existing != cls:
                logging.info(
                    f"Preprocessor id collision: '{identifier}' was already registered "
                    f"by {existing.__module__}.{existing.__qualname__}. "
                    f"Overwriting with {cls.__module__}.{cls.__qualname__}."
                )

        _PREPROCESSOR_REGISTRY[identifier] = cls
        return cls

    return decorator


def _lookup_class(serialization_id: str):
    """Look up a preprocessor class by its serialization ID.

    Args:
        serialization_id: The serialization ID of the preprocessor (either explicit or class name)

    Returns:
        The registered preprocessor class

    Raises:
        UnknownPreprocessorError: If the serialization ID is not registered
    """
    if serialization_id not in _PREPROCESSOR_REGISTRY:
        raise UnknownPreprocessorError(serialization_id)
    return _PREPROCESSOR_REGISTRY[serialization_id]
