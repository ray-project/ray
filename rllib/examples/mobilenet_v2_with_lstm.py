# Explains/tests Issues:
# https://github.com/ray-project/ray/issues/6928
# https://github.com/ray-project/ray/issues/6732

import argparse
from gym.spaces import Discrete, Box
import numpy as np

from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.examples.models.mobilenet_v2_with_lstm_models import \
    MobileV2PlusRNNModel, TorchMobileV2PlusRNNModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

cnn_shape = (4, 4, 3)
# The torch version of MobileNetV2 does channels first.
cnn_shape_torch = (3, 224, 224)

parser = argparse.ArgumentParser()
parser.add_argument("--torch", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()

    # Register our custom model.
    ModelCatalog.register_custom_model(
        "my_model", TorchMobileV2PlusRNNModel
        if args.torch else MobileV2PlusRNNModel)

    # Configure our Trainer.
    config = {
        "framework": "torch" if args.torch else "tf",
        "model": {
            "custom_model": "my_model",
            # Extra config passed to the custom model's c'tor as kwargs.
            "custom_model_config": {
                "cnn_shape": cnn_shape_torch if args.torch else cnn_shape,
            },
            "max_seq_len": 20,
        },
        "vf_share_layers": True,
        "num_workers": 0,  # no parallelism
        "env_config": {
            "action_space": Discrete(2),
            # Test a simple Image observation space.
            "observation_space": Box(
                0.0,
                1.0,
                shape=cnn_shape_torch if args.torch else cnn_shape,
                dtype=np.float32)
        },
    }

    trainer = PPOTrainer(config=config, env=RandomEnv)
    print(trainer.train())
