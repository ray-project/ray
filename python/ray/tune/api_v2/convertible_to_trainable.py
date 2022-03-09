# ONLY PLACEHOLDER TO BE REMOVED!!
import abc
from typing import Type

from ray.tune import Trainable


class ConvertibleToTrainable(abc.ABC):
    def as_trainable(self) -> Type["Trainable"]:
        raise NotImplementedError
