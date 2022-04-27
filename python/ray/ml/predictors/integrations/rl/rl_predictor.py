import os
from typing import Optional

import numpy as np
import pandas as pd
import ray.cloudpickle as cpickle
from ray.ml import Preprocessor, Checkpoint
from ray.ml.constants import PREPROCESSOR_KEY
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.train.integrations.rl.rl_trainer import (
    RL_TRAINER_CLASS_FILE,
    RL_CONFIG_FILE,
)
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
        cls, checkpoint: Checkpoint, env: Optional[EnvType] = None, **kwargs
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
        with checkpoint.as_directory() as checkpoint_path:
            trainer_class_path = os.path.join(checkpoint_path, RL_TRAINER_CLASS_FILE)
            config_path = os.path.join(checkpoint_path, RL_CONFIG_FILE)
            preprocessor_path = os.path.join(checkpoint_path, PREPROCESSOR_KEY)

            if not os.path.exists(trainer_class_path):
                raise ValueError(
                    f"RLPredictor only works with checkpoints created by "
                    f"RLTrainer. The checkpoint you specified is missing the "
                    f"`{RL_TRAINER_CLASS_FILE}` file."
                )

            if not os.path.exists(config_path):
                raise ValueError(
                    f"RLPredictor only works with checkpoints created by "
                    f"RLTrainer. The checkpoint you specified is missing the "
                    f"`{RL_CONFIG_FILE}` file."
                )

            with open(trainer_class_path, "rb") as fp:
                trainer_cls = cpickle.load(fp)

            with open(config_path, "rb") as fp:
                config = cpickle.load(fp)

            checkpoint_data_path = None
            for file in os.listdir(checkpoint_path):
                if file.startswith("checkpoint") and not file.endswith(
                    ".tune_metadata"
                ):
                    checkpoint_data_path = os.path.join(checkpoint_path, file)

            if not checkpoint_data_path:
                raise ValueError(
                    f"Could not find checkpoint data in RLlib checkpoint. "
                    f"Found files: {list(os.listdir(checkpoint_path))}"
                )

            if os.path.exists(preprocessor_path):
                with open(preprocessor_path, "rb") as fp:
                    preprocessor = cpickle.load(fp)
            else:
                preprocessor = None

            config.get("evaluation_config", {}).pop("in_evaluation", None)
            trainer = trainer_cls(config=config, env=env)
            trainer.restore(checkpoint_data_path)

            policy = trainer.get_policy()

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

        return actions
