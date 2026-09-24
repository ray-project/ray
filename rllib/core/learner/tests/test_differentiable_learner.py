import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.core.learner.differentiable_learner_config import (
    DifferentiableLearnerConfig,
)
from ray.rllib.core.learner.torch.torch_differentiable_learner import (
    TorchDifferentiableLearner,
)
from ray.rllib.core.learner.training_data import TrainingData
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.testing.testing_learner import BaseTestingAlgorithmConfig
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME,
)

torch, _ = try_import_torch()


class _TestingDifferentiableLearner(TorchDifferentiableLearner):
    """The smallest concrete learner: the loss is never reached by these tests."""

    def compute_loss_for_module(self, *, module_id, config, batch, fwd_out):
        return torch.tensor(0.0)


def _module_batch(num_rows):
    return SampleBatch({"obs": np.zeros((num_rows, 4), dtype=np.float32)})


class TestDifferentiableLearnerSkip(unittest.TestCase):
    """How a `DifferentiableLearner` handles modules it has no data for.

    It runs inside the meta-learner's inner loop and computes gradients with
    `torch.autograd.grad`, which engages no collective, so it decides alone -- the
    same position a `Learner` is in without peers, and it makes the same decisions:
    drop a module that has no rows, skip the update when nothing is left.
    """

    ENV = gym.make("CartPole-v1")

    def _build_learner(self):
        """A learner holding two real modules, as a multi-agent shard would."""
        config = BaseTestingAlgorithmConfig()
        module_spec = config.get_rl_module_spec(env=self.ENV)
        return _TestingDifferentiableLearner(
            config=config,
            learner_config=DifferentiableLearnerConfig(
                learner_class=_TestingDifferentiableLearner
            ),
            module=MultiRLModuleSpec(
                rl_module_specs={"m1": module_spec, "m2": module_spec}
            ).build(),
        )

    def test_module_without_rows_is_dropped(self):
        """The modules that do have data must still train."""
        learner = self._build_learner()
        batch = MultiAgentBatch(
            {"m1": _module_batch(129), "m2": _module_batch(0)},
            # As a shard reports it: the env steps of whichever module came last.
            env_steps=0,
        )

        minibatches = list(
            learner._create_iterator_if_necessary(
                training_data=TrainingData(batch=batch),
                num_epochs=1,
                minibatch_size=32,
            )
        )

        # ceil(129 / 32) = 5 minibatches of the one module that has rows.
        self.assertEqual(5, len(minibatches))
        for minibatch in minibatches:
            self.assertEqual(["m1"], list(minibatch.policy_batches.keys()))
            self.assertEqual(32, len(minibatch.policy_batches["m1"]))

    def test_update_is_skipped_when_no_module_has_rows(self):
        """With nothing left to train on, the inner update is skipped and counted."""
        learner = self._build_learner()
        batch = MultiAgentBatch(
            {"m1": _module_batch(0), "m2": _module_batch(0)}, env_steps=0
        )

        self.assertIsNone(
            learner._create_iterator_if_necessary(
                training_data=TrainingData(batch=batch),
                num_epochs=1,
                minibatch_size=32,
            )
        )
        self.assertEqual(
            1,
            learner.metrics.peek(
                (ALL_MODULES, LEARNER_UPDATE_SKIPPED_EMPTY_BATCH_LIFETIME)
            ),
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
