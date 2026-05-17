import os
from pathlib import Path


def __getattr__(name):
    if name == "sdk":
        from ray.autoscaler import sdk

        return sdk
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["sdk"]

AUTOSCALER_DIR_PATH = Path(os.path.abspath(os.path.dirname(__file__)))
