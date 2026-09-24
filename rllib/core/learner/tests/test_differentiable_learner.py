import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.core.columns import Columns
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
    """The smallest concrete learner: a behavior-cloning loss on the module's logits."""

    def compute_loss_for_module(self, *, module_id, config, batch, fwd_out):
        action_dist = (
            self.module[module_id]
            .get_train_action_dist_cls()
            .from_logits(fwd_out[Columns.ACTION_DIST_INPUTS])
        )
        return -torch.mean(action_dist.logp(batch[Columns.ACTIONS]))


def _batch(rows_per_module, *, env_steps=0):
    """A batch of random CartPole observations and actions, `rows` per module.

    `env_steps` defaults to what a shard reports: the env steps of whichever module
    came last, so a shard whose last module has no rows reports 0.
    """
    rng = np.random.default_rng(0)
    return MultiAgentBatch(
        {
            module_id: SampleBatch(
                {
                    Columns.OBS: rng.standard_normal((rows, 4), dtype=np.float32),
                    Columns.ACTIONS: rng.integers(0, 2, size=(rows,)),
                }
            )
            for module_id, rows in rows_per_module.items()
        },
        env_steps=env_steps,
    )


def _clone_params(learner):
    """What `TorchMetaLearner._clone_named_parameters` hands an inner learner."""
    return {
        module_id: {
            name: param.clone()
            for name, param in learner.module[module_id].named_parameters()
        }
        for module_id in learner.module.keys()
    }


class TestDifferentiableLearnerSkip(unittest.TestCase):
    """How a `DifferentiableLearner` handles modules it has no data for.

    It runs inside the meta-learner's inner loop and computes gradients with
    `torch.autograd.grad`, which engages no collective, so it decides alone -- the
    same position a `Learner` is in without peers, and it makes the same decisions:
    drop a module that has no rows, skip the update when nothing is left.
    """

    ENV = gym.make("CartPole-v1")

    def _build_learner(self, minibatch_size=32):
        """A learner holding two real modules, as a multi-agent shard would."""
        config = BaseTestingAlgorithmConfig()
        module_spec = config.get_rl_module_spec(env=self.ENV)
        learner = _TestingDifferentiableLearner(
            config=config,
            learner_config=DifferentiableLearnerConfig(
                learner_class=_TestingDifferentiableLearner,
                minibatch_size=minibatch_size,
                # One step is one gradient: large enough to tell a step, and the
                # gradient behind it, from float32 noise.
                lr=1.0,
            ),
            module=MultiRLModuleSpec(
                rl_module_specs={"m1": module_spec, "m2": module_spec}
            ).build(),
        )
        learner.build()
        return learner

    def test_module_without_rows_is_dropped(self):
        """The modules that do have data must still train; the others pass through."""
        learner = self._build_learner()

        minibatches = list(
            learner._create_iterator_if_necessary(
                training_data=TrainingData(batch=_batch({"m1": 129, "m2": 0})),
                num_epochs=1,
                minibatch_size=32,
            )
        )

        # ceil(129 / 32) = 5 minibatches of the one module that has rows.
        self.assertEqual(5, len(minibatches))
        for minibatch in minibatches:
            self.assertEqual(["m1"], list(minibatch.policy_batches.keys()))
            self.assertEqual(32, len(minibatch.policy_batches["m1"]))

        # The functional update must get along without `m2` as well: forward, loss
        # and gradients cover `m1` only, and `m2`'s parameters come back untouched.
        params = _clone_params(learner)
        updated_params, loss_per_module, _ = learner.update(
            params=params,
            training_data=TrainingData(batch=_batch({"m1": 129, "m2": 0})),
        )
        self.assertEqual(["m1"], list(loss_per_module))
        self.assertTrue(
            any(
                not torch.equal(updated_params["m1"][name], param)
                for name, param in params["m1"].items()
            )
        )
        for name, param in params["m2"].items():
            self.assertTrue(torch.equal(updated_params["m2"][name], param))

    def test_each_module_is_stepped_by_its_own_gradient(self):
        """With data for every module, each takes one step along its own gradient.

        `torch.autograd.grad` returns the gradients of all modules' parameters as one
        flat tuple; mapping them back must continue where the previous module's
        left off, not restart at the first gradient for every module.
        """
        learner = self._build_learner(minibatch_size=None)  # One step, whole batch.
        params = _clone_params(learner)

        updated_params, _, _ = learner.update(
            params=params,
            training_data=TrainingData(batch=_batch({"m1": 64, "m2": 64})),
        )

        tensor_batch = learner._convert_batch_type(_batch({"m1": 64, "m2": 64}))
        for module_id in ("m1", "m2"):
            module_batch = tensor_batch.policy_batches[module_id]
            fwd_out = torch.func.functional_call(
                learner.module[module_id], params[module_id], module_batch
            )
            loss = learner.compute_loss_for_module(
                module_id=module_id, config=None, batch=module_batch, fwd_out=fwd_out
            )
            grads = torch.autograd.grad(loss, list(params[module_id].values()))
            for (name, param), grad in zip(params[module_id].items(), grads):
                torch.testing.assert_close(
                    updated_params[module_id][name],
                    param - learner.learner_config.lr * grad,
                )

    def test_update_is_skipped_when_no_module_has_rows(self):
        """With nothing left to train on, the inner update is skipped and counted."""
        learner = self._build_learner()

        self.assertIsNone(
            learner._create_iterator_if_necessary(
                training_data=TrainingData(batch=_batch({"m1": 0, "m2": 0})),
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
