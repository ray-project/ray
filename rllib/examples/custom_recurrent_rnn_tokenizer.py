# @OldAPIStack

"""Example of defining custom tokenizers for recurrent models in RLModules.

This example shows the following steps:
- Define a custom tokenizer for a recurrent encoder.
- Define a model config that builds the custom tokenizer.
- Modify the default PPOCatalog to use the custom tokenizer config.
- Run a training that uses the custom tokenizer.
"""

import argparse
import os

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.tune.registry import register_env
from ray.rllib.examples.envs.classes.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.envs.classes.repeat_initial_obs_env import RepeatInitialObsEnv
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch
from dataclasses import dataclass
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.base import Encoder, ENCODER_OUT
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.rllib.core.models.configs import ModelConfig

parser = argparse.ArgumentParser()

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

parser.add_argument("--env", type=str, default="RepeatAfterMeEnv")
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=100, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=90.0, help="Reward at which we stop training."
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)

# We first define a custom tokenizer that we want to use to encode the
# observations before they are passed into the recurrent cells.
# We do this step for tf and for torch here to make the following steps framework-
# agnostic. However, if you use only one framework, you can skip the other one.


class CustomTorchTokenizer(TorchModel, Encoder):
    def __init__(self, config) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)
        self.net = nn.Sequential(
            nn.Linear(config.input_dims[0], config.output_dims[0]),
        )

    # Since we use this model as a tokenizer, we need to define it's output
    # dimensions so that we know the input dim for the recurent cells that follow.
    def get_output_specs(self):
        # In this example, the output dim will be 64, but we still fetch it from
        # config so that this code is more reusable.
        output_dim = self.config.output_dims[0]
        return SpecDict(
            {ENCODER_OUT: TensorSpec("b, d", d=output_dim, framework="torch")}
        )

    def _forward(self, inputs: dict, **kwargs):
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


class CustomTfTokenizer(TfModel, Encoder):
    def __init__(self, config) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        self.net = tf.keras.models.Sequential(
            [
                tf.keras.layers.Input(shape=config.input_dims),
                tf.keras.layers.Dense(config.output_dims[0], activation="relu"),
            ]
        )

    def get_output_specs(self):
        output_dim = self.config.output_dims[0]
        return SpecDict(
            {ENCODER_OUT: TensorSpec("b, d", d=output_dim, framework="tf2")}
        )

    def _forward(self, inputs: dict, **kwargs):
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


# Since RLlib decides during runtime which framework we use, we need to provide a
# model config that is buildable depending on the framework. The recurrent models
# will consume this config during runtime and build our custom tokenizer accordingly.


@dataclass
class CustomTokenizerConfig(ModelConfig):
    output_dims: tuple = None

    def build(self, framework):
        if framework == "torch":
            return CustomTorchTokenizer(self)
        else:
            return CustomTfTokenizer(self)


# We now modify the default Catalog for PPO to inject our config.
# Alternatively, we could inherit from the PPO RLModule here, which is more
# straightforward if we want to completely replace
# the default models. However, we want to keep RLlib's default LSTM Encoder and only
# place our tokenizer inside of it, so we use the Catalog here for demonstration
# purposes.


class CustomPPOCatalog(PPOCatalog):
    # Note that RLlib expects this to be a classmethod.
    @classmethod
    def get_tokenizer_config(
        cls,
        observation_space,
        model_config_dict,
        view_requirements=None,
    ) -> ModelConfig:
        return CustomTokenizerConfig(
            input_dims=observation_space.shape,
            output_dims=(64,),
        )


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)
    register_env("RepeatAfterMeEnv", lambda c: RepeatAfterMeEnv(c))
    register_env("RepeatInitialObsEnv", lambda _: RepeatInitialObsEnv())

    config = (
        PPOConfig()
        .environment(args.env, env_config={"repeat_delay": 2})
        .framework(args.framework)
        .env_runners(num_env_runners=0, num_envs_per_env_runner=20)
        .training(
            model={
                "vf_share_layers": False,
                "use_lstm": True,
                "lstm_cell_size": 256,
                "fcnet_hiddens": [256],
            },
            gamma=0.9,
            entropy_coeff=0.001,
            vf_loss_coeff=1e-5,
        )
        .rl_module(
            rl_module_spec=SingleAgentRLModuleSpec(catalog_class=CustomPPOCatalog)
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    }

    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
