"""
TensorFlow policy class used for PG.
"""

import logging
from typing import Dict, List, Type, Union, Optional, Tuple

from ray.rllib.evaluation.episode import Episode
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.algorithms.pg.pg import PGConfig
from ray.rllib.algorithms.pg.utils import post_process_advantages
from ray.rllib.utils.typing import AgentID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import (
    TFPolicyV2Type,
)
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import LearningRateSchedule
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()
logger = logging.getLogger(__name__)


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_pg_tf_policy(name: str, base: TFPolicyV2Type) -> TFPolicyV2Type:
    """Construct a PGTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with PGTrainer.
    """

    class PGTFPolicy(
        LearningRateSchedule,
        base,
    ):
        def __init__(
            self,
            observation_space,
            action_space,
            config: PGConfig,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            # Enforce AlgorithmConfig for PG Policies.
            if isinstance(config, dict):
                config = PGConfig.from_dict(config)

            # Initialize base class.
            base.__init__(
                self,
                observation_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            LearningRateSchedule.__init__(self, config.lr, config.lr_schedule)

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

        @override(base)
        def loss(
            self,
            model: ModelV2,
            dist_class: Type[ActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            """The basic policy gradients loss function.

            Calculates the vanilla policy gradient loss based on:
            L = -E[ log(pi(a|s)) * A]

            Args:
                model: The Model to calculate the loss for.
                dist_class: The action distr. class.
                train_batch: The training data.

            Returns:
                Union[TensorType, List[TensorType]]: A single loss tensor or a list
                    of loss tensors.
            """
            # Pass the training data through our model to get distribution parameters.
            dist_inputs, _ = model(train_batch)

            # Create an action distribution object.
            action_dist = dist_class(dist_inputs, model)

            # Calculate the vanilla PG loss based on:
            # L = -E[ log(pi(a|s)) * A]
            loss = -tf.reduce_mean(
                action_dist.logp(train_batch[SampleBatch.ACTIONS])
                * tf.cast(train_batch[Postprocessing.ADVANTAGES], dtype=tf.float32)
            )

            self.policy_loss = loss

            return loss

        @override(base)
        def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[
                Dict[AgentID, Tuple["Policy", SampleBatch]]
            ] = None,
            episode: Optional["Episode"] = None,
        ) -> SampleBatch:
            sample_batch = super().postprocess_trajectory(
                sample_batch, other_agent_batches, episode
            )
            return post_process_advantages(
                self, sample_batch, other_agent_batches, episode
            )

        @override(base)
        def extra_learn_fetches_fn(self) -> Dict[str, TensorType]:
            return {
                "learner_stats": {"cur_lr": self.cur_lr},
            }

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            """Returns the calculated loss and learning rate in a stats dict.

            Args:
                policy: The Policy object.
                train_batch: The data used for training.

            Returns:
                Dict[str, TensorType]: The stats dict.
            """

            return {
                "policy_loss": self.policy_loss,
                "cur_lr": self.cur_lr,
            }

    PGTFPolicy.__name__ = name
    PGTFPolicy.__qualname__ = name

    return PGTFPolicy


PGTF1Policy = get_pg_tf_policy("PGTF1Policy", DynamicTFPolicyV2)
PGTF2Policy = get_pg_tf_policy("PGTF2Policy", EagerTFPolicyV2)
