import os
from importlib import import_module
from pathlib import Path


# Lazily import sdk to avoid circular dependencies.
def __getattr__(name):
    if name == "sdk":
        sdk = import_module("ray.autoscaler.sdk")
        globals()[name] = sdk
        return sdk

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["sdk"]

AUTOSCALER_DIR_PATH = Path(os.path.abspath(os.path.dirname(__file__)))
