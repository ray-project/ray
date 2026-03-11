"""Annotation stubs for the Rust backend.

Provides PublicAPI decorator as a no-op since it's only used for documentation.
"""


def PublicAPI(*args, **kwargs):
    """No-op decorator matching the C++ backend's PublicAPI."""
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]

    def decorator(func):
        return func

    return decorator
