import unittest

import gym
import numpy as np
import itertools

from gymnasium.spaces import Box

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.configs import MLPEncoderConfig, CNNEncoderConfig


class TestCatalog(unittest.TestCase):
    def test_get_encoder_config(self):
        """Tests if we can create a bunch of encoders from the base catalog class."""

        # TODO (Artur): Add support for the commented out spaces
        input_spaces_and_config_types = [
            (Box(-1.0, 1.0, (5,), dtype=np.float32), MLPEncoderConfig),
            (Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32), CNNEncoderConfig),
            (Box(-1.0, 1.0, (240, 320, 3), dtype=np.float32), CNNEncoderConfig),
            # Box(-1.0, 1.0, (5, 5), dtype=np.float32),
            # MultiBinary([3, 10, 10]),
            # Discrete(5),
            # Tuple([Discrete(10), Box(-1.0, 1.0, (5,), dtype=np.float32)]),
            # Dict(
            #     {
            #         "task": Discrete(10),
            #         "position": Box(-1.0, 1.0, (5,), dtype=np.float32),
            #     }
            # ),
            # Text(),
            # Graph(),
            # GraphInstance(),
            # MultiDiscrete(),
            # Sequence(),
        ]

        # TODO (Artur): Add support for the commented out configs
        model_config_dicts = [
            # This should produce an MLPEncoder with three hidden layers
            {
                "fcnet_activation": "relu",
                "fcnet_hiddens": [256, 256, 256],
            },
            # This should produce an MLPEncoder with one hidden layer
            {
                "fcnet_hiddens": [512],
                "fcnet_activation": "relu",
            },
            # This should produce an LSTMEncoder with one hidden layer
            # {"use_lstm": True},
            # This should produce an AttentionNetEncoder with default configuration
            # {"use_attention": True, "attention_num_transformer_units": 4},
            # This should produce an AttentionNetEncoder with one hidden layer and
            # other custom configuration
            # {
            #     "fcnet_hiddens": [32],
            #     "fcnet_activation": "linear",
            #     "vf_share_layers": True,
            #     "use_attention": True,
            #     "max_seq_len": 10,
            #     "attention_num_transformer_units": 1,
            #     "attention_dim": 32,
            #     "attention_memory_inference": 10,
            #     "attention_memory_training": 10,
            #     "attention_num_heads": 1,
            #     "attention_head_dim": 32,
            #     "attention_position_wise_mlp_dim": 32,
            # },
            # This should produce an LSTMEncoder wrapping an CNNEncoder with
            # additional other custom configuration
            # {
            #     "use_lstm": True,
            #     "conv_activation": "elu",
            #     "dim": 42,
            #     "grayscale": True,
            #     "zero_mean": False,
            #     # Reduced channel depth and kernel size from default
            #     "conv_filters": [
            #         [32, [3, 3], 2],
            #         [32, [3, 3], 2],
            #         [32, [3, 3], 2],
            #         [32, [3, 3], 2],
            #     ]
            # }
        ]

        frameworks = ["tf", "torch"]

        # First check if encoders can be created for non-composite spaces
        print("Testing encoders for non-composite input spaces...")
        config_combinations = [
            frameworks,
            input_spaces_and_config_types,
            model_config_dicts,
        ]
        for config in itertools.product(*config_combinations):
            framework, input_space_and_config_type, model_config_dict = config
            input_space, model_config_dict_type = input_space_and_config_type
            if model_config_dict_type is not MLPEncoderConfig and framework == "tf":
                # TODO (Artur): Enable this once we have TF implementations
                continue
            print(
                f"Testing framework: \n{framework}\n, input space: \n{input_space}\n "
                f"and config: \n{model_config_dict}\n"
            )
            catalog = Catalog(
                observation_space=input_space,
                # Action space does not matter for encoders
                action_space=gym.spaces.Box(1, 1, (1,)),
                model_config_dict=model_config_dict,
                # TODO(Artur): Add view requirements when we need them
                view_requirements=None,
            )

            model_config_dict = catalog.get_encoder_config(
                observation_space=input_space, model_config_dict=model_config_dict
            )
            assert type(model_config_dict) == model_config_dict_type
            model_config_dict.build(framework=framework)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
