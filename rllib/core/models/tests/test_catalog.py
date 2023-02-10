import unittest
import numpy as np
import itertools

from gymnasium.spaces import Box

from ray.rllib.core.models.catalog import Catalog


class TestCatalog(unittest.TestCase):
    def test_get_primitive_model_config(self):
        """Tests if we can create a bunch of models from the base catalog class."""

        # TODO (Artur): Add support for the commented out spaces
        input_spaces = [
            Box(-1.0, 1.0, (5,), dtype=np.float32),
            # Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
            # Box(-1.0, 1.0, (240, 320, 3), dtype=np.float32),
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
        model_configs = [
            {
                "fcnet_activation": "relu",
                "fcnet_hiddens": [256, 256, 256],
            },
            {
                "fcnet_hiddens": [512],
                "fcnet_activation": "relu",
            },
            # {"use_lstm": True, "lstm_cell_size": 256},
            # {"use_attention": True, "attention_num_transformer_units": 4},
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

        config_combinations = [input_spaces, model_configs, frameworks]
        for config in itertools.product(*config_combinations):
            framework, input_space, model_config_dict = config
            print(
                f"Testing framework: \n{framework}\n, input space: \n{input_space}\n "
                f"and config: \n{model_config_dict}\n"
            )
            model_config = Catalog.get_primitive_model_config(
                input_space=input_space, model_config=model_config_dict
            )
            model_config.build(framework=framework)

    def test_get_encoder_config(self):
        """Tests if we can create a bunch of encoders from the base catalog class."""

        # TODO (Artur): Add support for the commented out spaces
        input_spaces = [
            Box(-1.0, 1.0, (5,), dtype=np.float32),
            # Box(-1.0, 1.0, (84, 84, 1), dtype=np.float32),
            # Box(-1.0, 1.0, (240, 320, 3), dtype=np.float32),
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
        model_configs = [
            {
                "fcnet_activation": "relu",
                "fcnet_hiddens": [256, 256, 256],
            },
            {
                "fcnet_hiddens": [512],
                "fcnet_activation": "relu",
            },
            # {"use_lstm": True, "lstm_cell_size": 256},
            # {"use_attention": True, "attention_num_transformer_units": 4},
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

        # First check if encoders can be created for singular spaces
        print("Testing encoders for singular input spaces...")
        config_combinations = [frameworks, input_spaces, model_configs]
        for config in itertools.product(*config_combinations):
            framework, input_space, model_config_dict = config
            print(
                f"Testing framework: \n{framework}\n, input space: \n{input_space}\n "
                f"and config: \n{model_config_dict}\n"
            )
            model_config = Catalog.get_encoder_config(
                observation_space=input_space, model_config=model_config_dict
            )
            model_config.build(framework=framework)

        # Secondly, check if composite input spaces can be created for composite spaces
        print("Testing encoders for composite input spaces...")
        # Produce all possible threefold combinations of the input spaces to test
        # flattened composite spaces.
        input_space_combination_permutations = list(
            itertools.permutations([input_spaces])
        )
        input_space_combinations = input_space_combination_permutations[:, 0:2]

        config_combinations = [frameworks, input_space_combinations, model_configs]
        for config in itertools.product(*config_combinations):
            framework, input_spaces, model_config_dict = config
            print(
                f"Testing framework: \n{framework}\n, input spaces: \n{input_spaces}\n "
                f"and config: \n{model_config_dict}\n"
            )
            model_config = Catalog.get_encoder_config(
                observation_space=input_spaces, model_config=model_config_dict
            )
            model_config.build(framework=framework)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
