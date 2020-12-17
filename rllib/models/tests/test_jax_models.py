from functools import partial
import numpy as np
from gym.spaces import Box, Dict, Tuple
from scipy.stats import beta, norm
import tree
import unittest

from ray.rllib.models.jax.jax_action_dist import JAXCategorical
from ray.rllib.models.tf.tf_action_dist import Beta, Categorical, \
    DiagGaussian, GumbelSoftmax, MultiActionDistribution, MultiCategorical, \
    SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchBeta, \
    TorchCategorical, TorchDiagGaussian, TorchMultiActionDistribution, \
    TorchMultiCategorical, TorchSquashedGaussian
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT, \
    softmax, SMALL_NUMBER, LARGE_INTEGER
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestJAXModels(unittest.TestCase):

    def test_jax_fcnet(self):
        """Tests the MultiActionDistribution (across all frameworks)."""
        batch_size = 1000
        input_space = Tuple([
            Box(-10.0, 10.0, shape=(batch_size, 4)),
            Box(-2.0, 2.0, shape=(
                batch_size,
                6,
            )),
            Dict({
                "a": Box(-1.0, 1.0, shape=(batch_size, 4))
            }),
        ])
        std_space = Box(
            -0.05, 0.05, shape=(
                batch_size,
                3,
            ))

        low, high = -1.0, 1.0
        value_space = Tuple([
            Box(0, 3, shape=(batch_size, ), dtype=np.int32),
            Box(-2.0, 2.0, shape=(batch_size, 3), dtype=np.float32),
            Dict({
                "a": Box(0.0, 1.0, shape=(batch_size, 2), dtype=np.float32)
            })
        ])

        for fw, sess in framework_iterator(session=True):
            if fw == "torch":
                cls = TorchMultiActionDistribution
                child_distr_cls = [
                    TorchCategorical, TorchDiagGaussian,
                    partial(TorchBeta, low=low, high=high)
                ]
            else:
                cls = MultiActionDistribution
                child_distr_cls = [
                    Categorical,
                    DiagGaussian,
                    partial(Beta, low=low, high=high),
                ]

            inputs = list(input_space.sample())
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4])

            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(inputs[2]["a"], np.log(SMALL_NUMBER),
                                     -np.log(SMALL_NUMBER))
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            # Sample deterministically.
            expected_det = [
                np.argmax(inputs[0], axis=-1),
                inputs[1][:, :3],  # [:3]=Mean values.
                # Mean for a Beta distribution:
                # 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2]["a"][:, 2:] / inputs[2]["a"][:, 0:2]))
                * (high - low) + low,
            ]
            out = distr.deterministic_sample()
            if sess:
                out = sess.run(out)
            check(out[0], expected_det[0])
            check(out[1], expected_det[1])
            check(out[2]["a"], expected_det[2])

            # Stochastic sampling -> expect roughly the mean.
            inputs = list(input_space.sample())
            # Fix categorical inputs (not needed for distribution itself, but
            # for our expectation calculations).
            inputs[0] = softmax(inputs[0], -1)
            # Fix std inputs (shouldn't be too large for this test).
            inputs[1][:, 3:] = std_space.sample()
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(inputs[2]["a"], np.log(SMALL_NUMBER),
                                     -np.log(SMALL_NUMBER))
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4])
            expected_mean = [
                np.mean(np.sum(inputs[0] * np.array([0, 1, 2, 3]), -1)),
                inputs[1][:, :3],  # [:3]=Mean values.
                # Mean for a Beta distribution:
                # 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2]["a"][:, 2:] / inputs[2]["a"][:, :2])) *
                (high - low) + low,
            ]
            out = distr.sample()
            if sess:
                out = sess.run(out)
            out = list(out)
            if fw == "torch":
                out[0] = out[0].numpy()
                out[1] = out[1].numpy()
                out[2]["a"] = out[2]["a"].numpy()
            check(np.mean(out[0]), expected_mean[0], decimals=1)
            check(np.mean(out[1], 0), np.mean(expected_mean[1], 0), decimals=1)
            check(
                np.mean(out[2]["a"], 0),
                np.mean(expected_mean[2], 0),
                decimals=1)

            # Test log-likelihood outputs.
            # Make sure beta-values are within 0.0 and 1.0 for the numpy
            # calculation (which doesn't have scaling).
            inputs = list(input_space.sample())
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(inputs[2]["a"], np.log(SMALL_NUMBER),
                                     -np.log(SMALL_NUMBER))
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4])
            inputs[0] = softmax(inputs[0], -1)
            values = list(value_space.sample())
            log_prob_beta = np.log(
                beta.pdf(values[2]["a"], inputs[2]["a"][:, :2],
                         inputs[2]["a"][:, 2:]))
            # Now do the up-scaling for [2] (beta values) to be between
            # low/high.
            values[2]["a"] = values[2]["a"] * (high - low) + low
            inputs[1][:, 3:] = np.exp(inputs[1][:, 3:])
            expected_log_llh = np.sum(
                np.concatenate([
                    np.expand_dims(
                        np.log(
                            [i[values[0][j]]
                             for j, i in enumerate(inputs[0])]), -1),
                    np.log(
                        norm.pdf(values[1], inputs[1][:, :3],
                                 inputs[1][:, 3:])), log_prob_beta
                ], -1), -1)

            values[0] = np.expand_dims(values[0], -1)
            if fw == "torch":
                values = tree.map_structure(lambda s: torch.Tensor(s), values)
            # Test all flattened input.
            concat = np.concatenate(tree.flatten(values),
                                    -1).astype(np.float32)
            out = distr.logp(concat)
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)
            # Test structured input.
            out = distr.logp(values)
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)
            # Test flattened input.
            out = distr.logp(tree.flatten(values))
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
