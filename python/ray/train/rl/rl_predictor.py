from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

from ray.air.checkpoint import Checkpoint
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import EnvType
from ray.train.predictor import Predictor
from ray.train.rl.rl_checkpoint import RLCheckpoint
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor


@PublicAPI(stability="alpha")
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
        preprocessor: Optional["Preprocessor"] = None,
    ):
        self.policy = policy
        super().__init__(preprocessor)

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
        checkpoint = RLCheckpoint.from_checkpoint(checkpoint)
        policy = checkpoint.get_policy(env)
        preprocessor = checkpoint.get_preprocessor()
        return cls(policy=policy, preprocessor=preprocessor)

    def _predict_pandas(self, data: "pd.DataFrame", **kwargs) -> "pd.DataFrame":
        if TENSOR_COLUMN_NAME in data:
            obs = data[TENSOR_COLUMN_NAME].to_numpy()
        else:
            obs = data.to_numpy()

        actions, _outs, _info = self.policy.compute_actions_from_input_dict(
            input_dict={"obs": obs}
        )

        return pd.DataFrame(np.array(actions))
