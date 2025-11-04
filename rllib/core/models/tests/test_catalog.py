import dataclasses
import functools
import itertools
import unittest
from collections import namedtuple

import gymnasium as gym
import numpy as np
import tree
from gymnasium.spaces import Box, Dict, Discrete, MultiDiscrete, Tuple

from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.distribution.torch.torch_distribution import (
    TorchCategorical,
    TorchDiagGaussian,
    TorchMultiCategorical,
    TorchMultiDistribution,
)
from ray.rllib.core.models.base import (
    ENCODER_OUT,
    Encoder,
)
from ray.rllib.core.models.catalog import (
    Catalog,
    _multi_action_dist_partial_helper,
    _multi_categorical_dist_partial_helper,
)
from ray.rllib.core.models.configs import (
    CNNEncoderConfig,
    MLPEncoderConfig,
    ModelConfig,
)
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import get_dummy_batch_for_space
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, _ = try_import_torch()


class TestCatalog(unittest.TestCase):
    def _check_model_outputs(self, model, framework, model_config_dict, input_space):
        """Checks the model's outputs for the given input space.

        Args:
            model: The model to check.
            framework: The framework to use (torch).
            model_config_dict: The model config dict to use.
            input_space: The input space to use.
        """
        convert_method = convert_to_torch_tensor
        expected_latent_dim = model_config_dict.get("latent_dim")
        if expected_latent_dim is None:
            # For CNNEncoders, `output_dims` are computed automatically.
            if isinstance(model.config, CNNEncoderConfig):
                expected_latent_dim = model.config.output_dims[0]
            # In order to stay backward compatible, we default to fcnet_hiddens[-1].
            # See MODEL_DEFAULTS for more details
            else:
                expected_latent_dim = model_config_dict["fcnet_hiddens"][-1]
        observations = convert_method(
            get_dummy_batch_for_space(input_space, batch_size=32)
        )
        states = tree.map_structure(
            lambda s: convert_method(32 * [s]), model.get_initial_state()
        )
        seq_lens = convert_method([32])
        inputs = {
            Columns.OBS: observations,
            Columns.STATE_IN: states,
            Columns.SEQ_LENS: seq_lens,
        }
        outputs = model(inputs)

        self.assertEqual(outputs[ENCODER_OUT].shape, (32, expected_latent_dim))
        if Columns.STATE_OUT in outputs:
            tree.map_structure_with_path(
                lambda p, v: (
                    True if v is None else self.assertEqual(v.shape, states[p].shape)
                ),
                outputs[Columns.STATE_OUT],
            )

    def test_atari_help(self):
        """Tests that we raise an appropriate error message if a user tries to create a
        Catalog object for an Atari game's observation space without providing a cnn
        config.
        """
        import pytest

        with pytest.raises(ValueError, match="This is the default atari obs shape."):
            Catalog(
                observation_space=Box(-np.inf, np.inf, [210, 160, 3], dtype=np.float32),
                action_space=Box(-1, 1, (1,)),
                model_config_dict={},
            )

    def test_get_encoder_config(self):
        """Tests if we can create a bunch of encoders from the base catalog class."""

        input_spaces_and_config_types = [
            (Box(-1.0, 1.0, (5,), dtype=np.float32), MLPEncoderConfig),
            (Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32), CNNEncoderConfig),
        ]

        model_configs = [
            # This should produce an MLPEncoder with three hidden layers
            DefaultModelConfig(
                fcnet_activation="relu",
                fcnet_hiddens=[256, 256, 256],
            ),
            # This should produce an MLPEncoder with one hidden layer
            DefaultModelConfig(
                fcnet_hiddens=[512, 512],
                fcnet_activation="relu",
            ),
        ]

        frameworks = ["torch"]

        # First check if encoders can be created for non-composite spaces
        print("Testing encoders for non-composite input spaces...")
        config_combinations = [
            frameworks,
            input_spaces_and_config_types,
            model_configs,
        ]
        for config in itertools.product(*config_combinations):
            framework, input_space_and_config_type, model_config = config
            model_config_dict = dataclasses.asdict(model_config)
            input_space, model_config_type = input_space_and_config_type
            print(
                f"Testing framework: \n{framework}\n, input space: \n{input_space}\n "
                f"and config: \n{model_config}\n"
            )
            catalog = Catalog(
                observation_space=input_space,
                # Action space does not matter for encoders
                action_space=gym.spaces.Box(1, 1, (1,)),
                model_config_dict=model_config_dict,
            )

            model_config = catalog._get_encoder_config(
                observation_space=input_space, model_config_dict=model_config_dict
            )
            self.assertEqual(type(model_config), model_config_type)
            model = model_config.build(framework=framework)

            # Do a forward pass and check if the output has the correct shape
            self._check_model_outputs(model, framework, model_config_dict, input_space)

    def test_get_dist_cls_from_action_space(self):
        """Tests if we can create a bunch of action distributions.

        Action distributions are created from the base catalog class. Things this
        test checks:
            - Whether we output the correct action distributions classes.
            - Whether we can instantiate the action distributions, query their
                required input dimensions and sample from them.

        """
        TestConfig = namedtuple(
            "TestConfig", ("action_space", "expected_dist_cls_dict")
        )
        test_configs = [
            # Box
            TestConfig(
                Box(-np.inf, np.inf, (7,), dtype=np.float32),
                {
                    "torch": TorchDiagGaussian,
                },
            ),
            # Discrete
            TestConfig(Discrete(5), {"torch": TorchCategorical}),
            # Nested Dict
            TestConfig(
                Dict(
                    {
                        "a": Box(-np.inf, np.inf, (7,), dtype=np.float32),
                        "b": Dict({"c": Discrete(5)}),
                    }
                ),
                {
                    "torch": TorchMultiDistribution,
                },
            ),
            # Nested Tuple
            TestConfig(
                Tuple(
                    (
                        Box(-np.inf, np.inf, (7,), dtype=np.float32),
                        Tuple((Discrete(5), Discrete(5))),
                    )
                ),
                {
                    "torch": TorchMultiDistribution,
                },
            ),
            # Tuple nested inside Dict
            TestConfig(
                Dict(
                    {
                        "a": Box(-np.inf, np.inf, (7,), dtype=np.float32),
                        "b": Dict(
                            {
                                "c": Tuple(
                                    (
                                        Box(-np.inf, np.inf, (7,), dtype=np.float32),
                                        Tuple((Discrete(5), Discrete(5))),
                                    )
                                )
                            }
                        ),
                    }
                ),
                {
                    "torch": TorchMultiDistribution,
                },
            ),
            # Dict nested inside Tuple
            TestConfig(
                Tuple(
                    (
                        Box(-np.inf, np.inf, (7,), dtype=np.float32),
                        Tuple(
                            (
                                Discrete(5),
                                Dict(
                                    {
                                        "a": Box(
                                            -np.inf, np.inf, (7,), dtype=np.float32
                                        ),
                                        "b": Dict({"c": Discrete(5)}),
                                    }
                                ),
                            )
                        ),
                    )
                ),
                {
                    "torch": TorchMultiDistribution,
                },
            ),
            # MultiDiscrete
            TestConfig(
                MultiDiscrete([5, 5, 5]),
                {"torch": TorchMultiCategorical},
            ),
        ]

        for (
            action_space,
            expected_cls_dict,
        ) in test_configs:
            print(f"Testing action space: {action_space}:")
            catalog = Catalog(
                observation_space=Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
                action_space=action_space,
                model_config_dict=MODEL_DEFAULTS.copy(),
            )

            dist_cls = catalog._get_dist_cls_from_action_space(
                action_space=action_space,
                framework="torch",
            )

            # Check if we can query the required input dimensions
            expected_cls = expected_cls_dict["torch"]
            if expected_cls is TorchMultiDistribution:
                # For these special cases, we need to create partials of the
                # expected classes so that we can calculate the required inputs
                expected_cls = _multi_action_dist_partial_helper(
                    catalog_cls=catalog,
                    action_space=action_space,
                    framework="torch",
                )
            elif expected_cls is TorchMultiCategorical:
                # For these special cases, we need to create partials of the
                # expected classes so that we can calculate the required inputs
                expected_cls = _multi_categorical_dist_partial_helper(
                    action_space=action_space, framework="torch"
                )

            # Now that we have sorted out special cases, we can finally get the
            # input_dim
            input_dim = expected_cls.required_input_dim(action_space)
            logits = np.ones((32, input_dim), dtype=np.float32)
            logits = torch.from_numpy(logits)
            # We don't need a model if we input tensors
            dist = dist_cls.from_logits(logits=logits)
            self.assertTrue(
                isinstance(dist, expected_cls_dict["torch"]),
                msg=f"Expected {expected_cls_dict['torch']}, " f"got {type(dist)}",
            )
            # Test if sampling works
            actions = dist.sample()
            # Test is logp works
            dist.logp(actions)

            # For any array of actions in a possibly nested space, convert to
            # numpy and pick the first one to check if it is in the action space.
            action = tree.map_structure(lambda a: convert_to_numpy(a)[0], actions)
            self.assertTrue(action_space.contains(action))

    def test_customize_catalog_from_algorithm_config(self):
        """Test if we can pass catalog to algorithm config and it ends up inside
        RLModule and is used to build models there."""

        class MyCatalog(PPOCatalog):
            def build_vf_head(self, framework):
                return torch.nn.Linear(self.latent_dims[0], 1)

        config = (
            PPOConfig()
            .rl_module(
                rl_module_spec=RLModuleSpec(catalog_class=MyCatalog),
            )
            .framework("torch")
        )

        algo = config.build(env="CartPole-v0")
        self.assertEqual(type(algo.get_module("default_policy").catalog), MyCatalog)

        # Test if we can pass custom catalog to algorithm config and train with it.
        config = (
            PPOConfig()
            .rl_module(
                rl_module_spec=RLModuleSpec(
                    module_class=PPOTorchRLModule, catalog_class=MyCatalog
                )
            )
            .framework("torch")
        )

        algo = config.build(env="CartPole-v0")
        algo.train()

    def test_post_init_overwrite(self):
        """Test if we can overwrite post_init method of a catalog class.

        This tests:
            - Defines a custom encoder and its config.
            - Defines a custom catalog class that uses the custom encoder by
                overwriting the __post_init__ method and defining a custom
                Catalog.encoder_config.
            - Defines a custom RLModule that uses the custom catalog.
            - Runs a forward pass through the custom RLModule to check if
                everything is working together as expected.

        """
        env = gym.make("CartPole-v0")

        class MyCostumTorchEncoderConfig(ModelConfig):
            def build(self, framework):
                return MyCostumTorchEncoder(self)

        class MyCostumTorchEncoder(TorchModel, Encoder):
            def __init__(self, config):
                super().__init__(config)
                self.net = torch.nn.Linear(env.observation_space.shape[0], 10)

            def _forward(self, input_dict, **kwargs):
                return {
                    ENCODER_OUT: (self.net(input_dict["obs"])),
                    Columns.STATE_OUT: None,
                }

        class MyCustomCatalog(PPOCatalog):
            def _determine_components(self):
                self._action_dist_class_fn = functools.partial(
                    self._get_dist_cls_from_action_space, action_space=self.action_space
                )
                self.latent_dims = (10,)
                self.encoder_config = MyCostumTorchEncoderConfig(
                    input_dims=self.observation_space.shape,
                )

        spec = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config=MODEL_DEFAULTS.copy(),
            catalog_class=MyCustomCatalog,
        )
        module = spec.build()

        module.forward_inference(
            batch={"obs": torch.ones((32, *env.observation_space.shape))}
        )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
