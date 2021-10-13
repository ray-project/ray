from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.tune.registry import register_env

# from ray.rllib.utils import try_import_tf
from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
from pettingzoo.butterfly import pistonball_v4
import supersuit as ss
from torch import nn
import os
import argparse

# This tutorial uses the PettingZoo (https://www.pettingzoo.ml/)
# Multi-Agent Environment API with RLlib.
# For more information, please see the PettingZoo Team's blog post at:
# https://tinyurl.com/pettingzoo-rllib-tutorial


class CNNModelV2(TorchModelV2, nn.Module):
    def __init__(self, obs_space, act_space, num_outputs, *args, **kwargs):
        TorchModelV2.__init__(self, obs_space, act_space, num_outputs, *args,
                              **kwargs)
        nn.Module.__init__(self)
        self.model = nn.Sequential(
            nn.Conv2d(3, 32, [8, 8], stride=(4, 4)),
            nn.ReLU(),
            nn.Conv2d(32, 64, [4, 4], stride=(2, 2)),
            nn.ReLU(),
            nn.Conv2d(64, 64, [3, 3], stride=(1, 1)),
            nn.ReLU(),
            nn.Flatten(),
            (nn.Linear(3136, 512)),
            nn.ReLU(),
        )
        self.policy_fn = nn.Linear(512, num_outputs)
        self.value_fn = nn.Linear(512, 1)

    def forward(self, input_dict, state, seq_lens):
        model_out = self.model(input_dict["obs"].permute(0, 3, 1, 2))
        self._value_out = self.value_fn(model_out)
        return self.policy_fn(model_out), state

    def value_function(self):
        return self._value_out.flatten()


# Function that outputs the environment you want to register
# We use SuperSuit to wrap the environment and preprocess observations
def env_creator(args):
    env = pistonball_v4.parallel_env(
        n_pistons=20,
        local_ratio=0,
        time_penalty=-0.1,
        continuous=True,
        random_drop=True,
        random_rotate=True,
        ball_mass=0.75,
        ball_friction=0.3,
        ball_elasticity=1.5,
        max_cycles=125)
    env = ss.color_reduction_v0(env, mode="B")
    env = ss.dtype_v0(env, "float32")
    env = ss.resize_v0(env, x_size=84, y_size=84)
    env = ss.frame_stack_v1(env, 3)
    env = ss.normalize_obs_v0(env, env_min=0, env_max=1)
    # env = ss.flatten_v0(env)
    return env


parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="torch",
    help="The DL framework specifier.")
parser.add_argument(
    "--timesteps",
    type=int,
    default=100000,
    help="Number of timesteps to train.")
parser.add_argument(
    "--checkpoint-freq",
    type=int,
    default=100,
    help="Checkpoint frequency during learning.")
parser.add_argument(
    "--log-level",
    choices=["ERROR", "WARNING", "INFO", "DEBUG"],
    default="ERROR",
    help="Which messages to log.")
parser.add_argument(
    "--num-workers",
    type=int,
    default=8,
    help="Number of RLlib workers during training.")
parser.add_argument(
    "--num-envs-per-workers",
    type=int,
    default=1,
    help="Number of environments per RLlib worker.")

if __name__ == "__main__":
    args = parser.parse_args()
    env_name = "pistonball_v4"

    register_env(env_name,
                 lambda config: ParallelPettingZooEnv(env_creator(config)))

    ModelCatalog.register_custom_model("CNNModelV2", CNNModelV2)

    tune.run(
        "PPO",
        name="PPO_pistonball",
        stop={"timesteps_total": args.timesteps},
        checkpoint_freq=args.checkpoint_freq,
        local_dir="~/ray_results/" + env_name,
        config={
            # Environment specific
            "env": env_name,
            # General
            "log_level": args.log_level,
            "framework": args.framework,
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "num_workers": args.num_workers,
            "num_envs_per_worker": args.num_envs_per_workers,
            "compress_observations": True,
            "batch_mode": "truncate_episodes",
            # Fragment length, collected from each worker and for each agent
            "rollout_fragment_length": 64,
            # train batch size, fragments are concatenated up to this point
            "train_batch_size": 1024,
            # Force reset environment after this many steps
            "horizon": 125,
            # minibatch size for PPO to optimize over
            "sgd_minibatch_size": 64,
            # number of optimization iterations over each minibatch
            "num_sgd_iter": 10,
            "lr": 5e-4,
            "model": {
                "custom_model": "CNNModelV2",
            },
            # "use_critic": True,
            "use_gae": True,
            "lambda": 0.9,
            "gamma": 0.99,
            # "kl_coeff": 0.001,
            # "kl_target": 1000.,
            "clip_param": 0.4,
            "grad_clip": None,
            "entropy_coeff": 0.1,
            "vf_loss_coeff": 0.25,
            "clip_actions": True,
            # Configuration for multiagent setup with policy sharing:
            "multiagent": {
                # Setup a single, shared policy for all agents: "policy_0".
                # Use a simple set of strings (PolicyID) here. RLlib will
                # automatically determine the policy class
                # (Trainer's default class), observation and action spaces
                # (inferred from the env), and config overrides ({} here)
                "policies": {"policy_0"},
                # Map all agents to the "policy_0" PolicyID.
                "policy_mapping_fn": lambda agent_id: "policy_0",
            },
        },
    )
