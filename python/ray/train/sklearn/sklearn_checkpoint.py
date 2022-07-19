import os
from typing import TYPE_CHECKING, Optional

from sklearn.base import BaseEstimator
from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
def SklearnCheckpoint(Checkpoint):
    @classmethod
    def from_estimator(
        cls,
        estimator: BaseEstimator,
        *,
        path: os.PathLike,
        preprocessor: Optional["Preprocessor"] = None,
    ) -> Checkpoint:
        raise NotImplementedError

    def get_model(self) -> BaseEstimator:
        raise NotImplementedError
