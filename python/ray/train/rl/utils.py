import os
from typing import Optional, Tuple, TYPE_CHECKING

import ray.cloudpickle as cpickle
from ray.air.checkpoint import Checkpoint
from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import EnvType

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

RL_TRAINER_CLASS_FILE = "trainer_class.pkl"
RL_CONFIG_FILE = "config.pkl"


def load_checkpoint(
    checkpoint: Checkpoint,
    env: Optional[EnvType] = None,
) -> Tuple[Policy, Optional["Preprocessor"]]:
    """Load a Checkpoint from ``RLTrainer``.

    Args:
        checkpoint: The checkpoint to load the policy and
            preprocessor from. It is expected to be from the result of a
            ``RLTrainer`` run.
        env: Optional environment to instantiate the trainer with. If not given,
            it is parsed from the saved trainer configuration instead.

    Returns:
        The policy and AIR preprocessor contained within.
    """
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
            if file.startswith("checkpoint") and not file.endswith(".tune_metadata"):
                checkpoint_data_path = os.path.join(checkpoint_path, file)

        if not checkpoint_data_path:
            raise ValueError(
                f"Could not find checkpoint data in RLlib checkpoint. "
                f"Found files: {list(os.listdir(checkpoint_path))}"
            )

        preprocessor = load_preprocessor_from_dir(checkpoint_path)

        config.get("evaluation_config", {}).pop("in_evaluation", None)
        trainer = trainer_cls(config=config, env=env)
        trainer.restore(checkpoint_data_path)

        policy = trainer.get_policy()
        return policy, preprocessor
