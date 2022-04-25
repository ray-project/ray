import os
from typing import Optional

import numpy as np
import pandas as pd
import ray.cloudpickle as cpickle
from ray.ml import Preprocessor, Checkpoint
from ray.ml.predictor import Predictor, DataBatchType
from ray.ml.train.integrations.rl.rl_trainer import (
    RL_TRAINER_CLASS_FILE,
    RL_CONFIG_FILE,
)
from ray.rllib.agents.trainer import Trainer as RLlibTrainer
from ray.rllib.utils.typing import EnvType


class RLPredictor(Predictor):
    def __init__(
        self,
        trainer: RLlibTrainer,
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.trainer = trainer
        self.preprocessor = preprocessor

    @classmethod
    def from_checkpoint(
        cls, checkpoint: Checkpoint, env: Optional[EnvType] = None, **kwargs
    ) -> "Predictor":
        with checkpoint.as_directory() as checkpoint_path:
            trainer_class_path = os.path.join(checkpoint_path, RL_TRAINER_CLASS_FILE)
            config_path = os.path.join(checkpoint_path, RL_CONFIG_FILE)

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
                if file.startswith("checkpoint"):
                    checkpoint_data_path = os.path.join(checkpoint_path, file)

            if not checkpoint_data_path:
                raise ValueError(
                    f"Could not find checkpoint data in RLlib checkpoint. "
                    f"Found files: {list(os.listdir(checkpoint_path))}"
                )

            config["evaluation_config"].pop("in_evaluation", None)
            trainer = trainer_cls(config=config, env=env)
            trainer.restore(checkpoint_data_path)

            return RLPredictor(trainer=trainer)

    def predict(self, data: DataBatchType, **kwargs) -> DataBatchType:
        if isinstance(data, pd.DataFrame):
            obs = data.to_numpy()
        elif isinstance(data, pd.Series):
            obs = data.to_numpy()
        elif isinstance(data, np.ndarray):
            obs = data.squeeze()
        elif isinstance(data, list):
            obs = np.array(data)
        else:
            raise RuntimeError("Invalid data type:", type(data))

        policy = self.trainer.get_policy()
        actions, _outs, _info = policy.compute_actions(obs)
        return actions
