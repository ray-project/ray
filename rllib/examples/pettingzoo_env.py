from ray import tune
from ray.rllib.models import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.tune.registry import register_env
# from ray.rllib.utils import try_import_tf
from ray.rllib.env.wrappers.pettingzoo_env import ParallelPettingZooEnv
from pettingzoo.butterfly import pistonball_v4
import supersuit as ss
from torch import nn

# This tutorial uses PettingZoo's Multi-Agent Environment API (https://www.pettingzoo.ml/)
# with RLlib. For more information, please see the blog post at:
# https://towardsdatascience.com/using-pettingzoo-with-rllib-for-multi-agent-deep-reinforcement-learning-5ff47c677abd


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


if __name__ == "__main__":
    env_name = "pistonball_v4"

    register_env(env_name,
                 lambda config: ParallelPettingZooEnv(env_creator(config)))

    test_env = ParallelPettingZooEnv(env_creator({}))
    obs_space = test_env.observation_space
    act_space = test_env.action_space

    ModelCatalog.register_custom_model("CNNModelV2", CNNModelV2)

    def gen_policy(i):
        config = {
            "model": {
                "custom_model": "CNNModelV2",
            },
            "gamma": 0.99,
        }
        return (None, obs_space, act_space, config)

    policies = {"policy_0": gen_policy(0)}

    policy_ids = list(policies.keys())

    tune.run(
        "PPO",
        name="PPO",
        stop={"timesteps_total": 5000000},
        checkpoint_freq=10,
        local_dir="~/ray_results/" + env_name,
        config={
            # Environment specific
            "env": env_name,
            # General
            "log_level": "ERROR",
            "framework": "torch",
            "num_gpus": 1,
            "num_workers": 4,
            "num_envs_per_worker": 1,
            "compress_observations": False,
            "batch_mode": "truncate_episodes",

            # "use_critic": True,
            "use_gae": True,
            "lambda": 0.9,
            "gamma": .99,

            # "kl_coeff": 0.001,
            # "kl_target": 1000.,
            "clip_param": 0.4,
            "grad_clip": None,
            "entropy_coeff": 0.1,
            "vf_loss_coeff": 0.25,
            "sgd_minibatch_size": 64,
            "num_sgd_iter": 10,  # epoc
            "rollout_fragment_length": 512,
            "train_batch_size": 512 * 4,
            "lr": 2e-05,
            "clip_actions": True,

            # Method specific
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": (lambda agent_id: policy_ids[0]),
            },
        },
    )
