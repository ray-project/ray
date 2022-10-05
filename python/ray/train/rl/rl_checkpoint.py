import os
from packaging import version
from typing import Optional

from ray.air.checkpoint import Checkpoint
import ray.cloudpickle as cpickle
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.checkpoints import get_checkpoint_info
from ray.rllib.utils.typing import EnvType
from ray.util.annotations import PublicAPI

RL_TRAINER_CLASS_FILE = "trainer_class.pkl"
RL_CONFIG_FILE = "config.pkl"


@PublicAPI(stability="alpha")
class RLCheckpoint(Checkpoint):
    """A :py:class:`~ray.air.checkpoint.Checkpoint` with RLlib-specific
    functionality.

    Create this from a generic :py:class:`~ray.air.checkpoint.Checkpoint` by calling
    ``RLCheckpoint.from_checkpoint(ckpt)``.
    """

    def get_policy(self, env: Optional[EnvType] = None) -> Policy:
        """Retrieve the policy stored in this checkpoint.

        Args:
            env: Optional environment to instantiate the trainer with. If not given,
                it is parsed from the saved trainer configuration.

        Returns:
            The policy stored in this checkpoint.
        """
        # TODO: Deprecate this RLCheckpoint class (or move all our
        #  Algorithm/Policy.from_checkpoint utils into here).
        # If newer checkpoint version -> Use `Policy.from_checkpoint()` util.
        checkpoint_info = get_checkpoint_info(checkpoint=self)
        if checkpoint_info["checkpoint_version"] > version.Version("0.1"):
            # Since we have an Algorithm checkpoint, will extract all policies in that
            # Algorithm -> need to index into "default_policy" in the returned dict.
            return Policy.from_checkpoint(checkpoint=self)["default_policy"]

        # Older checkpoint version.
        with self.as_directory() as checkpoint_path:
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
                if file.startswith("checkpoint") and not file.endswith(
                    ".tune_metadata"
                ):
                    checkpoint_data_path = os.path.join(checkpoint_path, file)

            if not checkpoint_data_path:
                raise ValueError(
                    f"Could not find checkpoint data in RLlib checkpoint. "
                    f"Found files: {list(os.listdir(checkpoint_path))}"
                )

            config.get("evaluation_config", {}).pop("in_evaluation", None)
            trainer = trainer_cls(config=config, env=env)
            trainer.restore(checkpoint_data_path)

            return trainer.get_policy()
