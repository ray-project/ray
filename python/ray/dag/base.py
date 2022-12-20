"""This module defines the base class for object scanning and gets rid of
reference cycles."""
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DAGNodeBase:
    """Common base class for a node in a Ray task graph."""
