from typing import Union, Type, Optional

from ray.ml.predictor import Predictor, DataBatchType
from ray.rllib.agents.trainer import Trainer as RLlibTrainer


class RLPredictor(Predictor):
    def __init__(
        self,
        algorithm: Union[str, Type[RLlibTrainer]],
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.model = model
        self.preprocessor = preprocessor

    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "Predictor":
        self.trainer = ppo.PPOTrainer(
            config={
                "framework": "torch",
                "num_workers": 0,
            },
            env="CartPole-v0",
        )
        self.trainer.restore(checkpoint_path)

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        obs = data.to_numpy()
        action = self.trainer.compute_single_action(obs)
        return int(action)
