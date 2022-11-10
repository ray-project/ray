"""This file contains temporary stubs for TensorDict, SpecDict, and ModelConfig.
This allows us to implement modules utilizing these APIs before the actual
changes land in master.
This file is to be removed once these modules are commited to master.
"""

from typing import Any


# TODO: Remove once TensorDict is in master
class TensorDict:
    def __init__(self, d=None, **kwargs):
        if d is None:
            d = {}
        self.d = d

    def filter(self, specs: "SpecDict") -> "TensorDict":
        return TensorDict({k: v for k, v in self.d.items() if k in specs.keys()})

    def __eq__(self, other: "TensorDict") -> bool:
        return True

    def __getitem__(self, idx):
        return self.d[idx]

    def __iter__(self):
        for k in self.d:
            yield k

    def flatten(self):
        return self

    def keys(self):
        return self.d.keys()


# TODO: Remove once SpecDict is in master
class SpecDict:
    def __init__(self, d=None, **kwargs):
        if d is None:
            d = {}
        self.d = d

    def keys(self):
        return self.d.keys()

    def validate(self, spec: Any) -> bool:
        return True


# TODO: Remove once ModelConfig is in master
class ModelConfig:
    name = "Bork"
