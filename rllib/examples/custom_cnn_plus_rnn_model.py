# Explains/tests Issues:
# https://github.com/ray-project/ray/issues/6928
# https://github.com/ray-project/ray/issues/6732

import argparse
from gym.spaces import Discrete, Box
import numpy as np

from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.examples.models.cnn_plus_rnn_model import CNNPlusRNNModel, \
    TorchCNNPlusRNNModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

cnn_shape = (4, 4, 3)

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our custom model.
    ModelCatalog.register_custom_model(
        "my_model", TorchCNNPlusRNNModel if args.torch else CNNPlusRNNModel)

    # Configure our Trainer.
    config = {
        "use_pytorch": args.torch,
        "model": {
            "custom_model": "my_model",
            # Extra config passed to the custom model's c'tor as kwargs.
            "custom_options": {
                "cnn_shape": cnn_shape,
            },
            "max_seq_len": 20,
        },
        "vf_share_layers": True,
        "num_workers": 0,  # no parallelism
        "env_config": {
            "action_space": Discrete(2),
            # Test a simple Tuple observation space.
            "observation_space": Box(
                0.0, 1.0, shape=cnn_shape, dtype=np.float32)
        },
    }

    trainer = PPOTrainer(config=config, env=RandomEnv)
    trainer.train()
