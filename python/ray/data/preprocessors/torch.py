from typing import TYPE_CHECKING, Callable, List
from ray.data.preprocessor import Preprocessor

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import torch


@PublicAPI(annotation="alpha")
class TorchPreprocessor(Preprocessor):
    def __init__(
        self, columns: List[str], transform: Callable[["torch.Tensor"], "torch.Tensor"]
    ):
        raise NotImplementedError

    def __repr__(self) -> str:
        raise NotImplementedError
