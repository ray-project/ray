import os

from ray.ml.trainer import Trainer
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class RLLibTrainer(Trainer):
    def training_loop(self) -> None:
        pass
