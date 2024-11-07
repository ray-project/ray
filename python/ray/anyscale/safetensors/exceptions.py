class SafetensorsError(Exception):
    """Base class for safetensors-related exceptions."""

    pass


class NotFoundError(SafetensorsError):
    pass
