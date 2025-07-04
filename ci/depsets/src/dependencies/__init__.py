"""Package for managing Python dependency sets."""

from dependencies.depset import DepSet, Dep
from dependencies.config import load_config, Config

__all__ = ["DepSet", "Dep", "load_config", "Config"]

# This file makes dependencies a Python package
