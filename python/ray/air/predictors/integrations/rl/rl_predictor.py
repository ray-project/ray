from typing import Optional

import numpy
import numpy as np
import pandas as pd
from ray.ml import Preprocessor, Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.train.integrations.rl import load_checkpoint
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import EnvType


class RLPredictor(Predictor):
    """A predictor for RLlib policies.

    Args:
        policy: The RLlib policy on which to perform inference on.
        preprocessor: A preprocessor used to transform data batches prior
            to prediction.
    """

    def __init__(
        self,
        policy: Policy,
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.policy = policy
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        env: Optional[EnvType] = None,
        **kwargs,
    ) -> "Predictor":
        """Create RLPredictor from checkpoint.

        This method requires that the checkpoint was created with the Ray AIR
        RLTrainer.

        Args:
            checkpoint: The checkpoint to load the model and
                preprocessor from.
            env: Optional environment to instantiate the trainer with. If not given,
                it is parsed from the saved trainer configuration instead.

        """
        policy, preprocessor = load_checkpoint(checkpoint, env)
        return RLPredictor(policy=policy, preprocessor=preprocessor)

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        if isinstance(data, pd.DataFrame):
            obs = data.to_numpy()
        elif isinstance(data, np.ndarray):
            obs = data
        elif isinstance(data, list):
            obs = np.array(data)
        else:
            raise RuntimeError("Invalid data type:", type(data))

        if self.preprocessor:
            obs = self.preprocessor.transform_batch(obs)

        actions, _outs, _info = self.policy.compute_actions_from_input_dict(
            input_dict={"obs": obs}
        )

        return np.array(actions)
