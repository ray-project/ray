"""Example of how to write a custom loss function, custom rl_module and custom config.

This script implements a new critic network that models that lets the critic know "what it doesn't know".
Epistemic uncertainty is lack of knowledge in the environment while aleatoric is natural, or inherent,
uncertainty. Further reading can be seen here: https://arxiv.org/pdf/2103.04047 @ page 51 with further
explanation of epistemic neural networks @ page 74.

The epinet (epistemic neural network) is a very efficient means of exploration for agents in high
epistemic environments. For example, an agent that is playing against another agent has a high-level
of uncertainty since it doesn't know about the environment, but it also doesn't know the adversary's 
policy. Therefore, the epinet does very well in adversarial enviornments. 

The epinet takes, in this example, the critic's features and puts them against a z-number of priors. 
For example, if there is a z-dim of 3 then we are changing the prior distribution  three times and
asking the network "are you sure?" over these three. If the network returns a close estimate in all
three scenarious the agent is "certain" and there is little exploration needed / epistemic uncertainty.
However, if the critic's returns are sporadic then this tells the agent something here is off and it
is uncertain of this given state. So explore this area more. This can be seen as "jostling" or 
"perturbing" the agent's state so it encompasses a wider latent space during training. 

As investigated in [Osband et al., 2021] from DeepMind's Efficient Agent Team, this exploration technique
typically will largely outperform large ensemble of models in image classification tasks. This is seen
in the metric of joint log-loss. In other words, the epinet focuses on joint predictions whereas typical
models do not.

Paper: https://arxiv.org/pdf/2107.08924

Moving into RL realm with DQN, [Osband et al., 2023] again showed that the typical very lightweight epinet
was able to achieve similar, if not better, results compared to large ensemble models at a fraction of the 
computation cost (~100 FLOPs vs ~3 FLOPs). Again, this was achieved because of joint predictions as seen 
by the normalized test log-loss which evaluates the quality of marginal and joint predictions of testing
environments.

Paper: https://arxiv.org/pdf/2302.09205

Key difference: While the paper uses DQN for discrete action spaces, this implementation took it a step further
and integrated it with PPO/continuous action spaces. This was rigorously tested in adversarial flight environments 
and showed a 3100%+ reward increase and a reduction of convergence time by 45%. 
Note: in terms of open-source, a basic scenario can be from PyFlyt's dogfighting environment seen here:
https://taijunjet.com/PyFlyt/documentation/pz_envs/ma_fixedwing_dogfight_env.html

The parameter --enn_num_layers is the number of layers for the epinet. Note: These are meant to be small networks
so this number, generally, should be 2.

The parameter --enn_layer_size is the size of the layers. Note: This is normally a hyperparameter where one can
tune between 25, 50, 75, but 50 has been shown to have generally good results.

The parameter --z_dim is the number of dimensions for the z-index (from the paper). This is the number of "priors"
that the agent is tested over and this is the direct parameter combines states to depend on one another. This
is a hyperparameter and can be on a wide range of 3 - 25, but generally 3-7 is a good starting point. For simple 
enviornments (MuJoCo) the recommendation is 3. For more complex environmnets like League play or SMAC it is recommended
to start at 5. 

This example shows:
    - how to subclass an existing (torch) Learner and override its `compute_loss_for_module()` method.
    - how you can add your own loss to a custom (or non-custom) RLModule using compute_loss_for_module.
    - how to add custom pipeline components to a custom config class using build_learner_connector.
    - how to plug in a custom Learner, RLModule, its loss, and use a custom config to train with.

See the :py:class:`~ray.rllib.examples.learners.classes.epinet_learner.PPOTorchLearnerWithEpinetLoss`  # noqa
class for details on how to override the main (PPO) loss function.

How to run this script
----------------------
`python epinet.py --enable-new-api-stack --enn_num_layers=2 --enn_layer_size=25 --z_dim=3 --lr=0.003`


Results to expect
-----------------
In general, this epinet is recommended to use with complex environments with high epistemic uncertainty. I.e.
an environment that has a lot of unknowns with multi-agents. When using this on simple environments this will
generally converge slower than a basic network, but it still will converge. For instance, the MuJoCo environments
are not good testing environments since their physics are deterministic and the uncertainty is mainly from
the model's weights (aleatoric uncertainty).
"""
import gymnasium as gym
from ray.rllib.examples.learners.classes.epinet_config import (
    PPOConfigWithEpinet,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
)
from ray.tune.registry import register_env
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.examples.learners.classes.epinet_rlm import EpinetTorchRLModule


torch, _ = try_import_torch()


class RewardWrapper(gym.RewardWrapper):
    def __init__(self, env):
        super().__init__(env)

    def reward(self, reward):
        # Scale rewards -- helps environment converge easier.
        if reward >= 99.0 or reward <= -99.0:
            return reward / 10
        return reward


def create_quadx_waypoints_env(env_config):
    import PyFlyt.gym_envs
    from PyFlyt.gym_envs import FlattenWaypointEnv

    env = gym.make("PyFlyt/QuadX-Waypoints-v4")
    # Wrap Environment to use max 10 and -10 for rewards
    env = RewardWrapper(env)

    return FlattenWaypointEnv(env, context_length=1)


parser = add_rllib_example_script_args()
parser.set_defaults(enable_new_api_stack=True)
parser.add_argument(
    "--enn_network",
    type=list,
    default=[25, 25],
    help="The epinet neural network architecture.",
)
parser.add_argument(
    "--z_dim",
    type=int,
    default=3,
    help="Number of z-dimensions for the epinet.",
)
parser.add_argument(
    "--lr",
    type=float,
    default=0.0003,
    help="Learning rate",
)

if __name__ == "__main__":
    args = parser.parse_args()
    register_env("QuadX_waypoints", env_creator=create_quadx_waypoints_env)
    assert (
        args.enable_new_api_stack
    ), "Must set --enable-new-api-stack when running this script!"
    assert args.algo == "PPO", "Must set --algo=PPO when running this script!"

    base_config = (
        PPOConfigWithEpinet()
        .environment(
            env="QuadX_waypoints",
            env_config={
                "sparse_reward": False,
                "num_targets": 4,
                "angle_representation": "quaternion",
            },
        )
        .env_runners(num_env_runners=5)
        .api_stack(enable_rl_module_and_learner=True)
        .training(
            train_batch_size_per_learner=2048,
            minibatch_size=256,
            lr=args.lr,
            gamma=0.99,
            lambda_=0.95,
            enn_network=args.enn_network,
            z_dim=args.z_dim,
            num_epochs=20,
            vf_loss_coeff=0.5,
            clip_param=0.2,
            vf_clip_param=10,
        )
        .rl_module(
            rl_module_spec=RLModuleSpec(
                module_class=EpinetTorchRLModule,
            )
        )
        .resources(num_gpus=1)
        .env_runners(preprocessor_pref="rllib")
        .reporting(keep_per_episode_custom_metrics=True)
    )

algo = base_config.build()
total_timesteps = 0
default_timesteps = 200_000
default_reward = 250
for iteration in range(1000):
    result = algo.train()
    reward = result["env_runners"]["module_episode_returns_mean"]["default_policy"]
    print(f"iteration: {iteration} with reward mean: {reward}")
algo.stop()
