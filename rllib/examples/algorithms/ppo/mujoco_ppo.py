"""Example showing how to train PPO on MuJoCo continuous control tasks.

This example demonstrates training PPO on MuJoCo physics simulation environments.
MuJoCo tasks involve continuous action spaces and require learning precise motor
control policies for simulated robots.

This example:
- Trains on Humanoid-v4 by default (configurable via --env flag)
- Uses tuned PPO hyperparameters for continuous control (high gamma, lambda)
- Samples from multiple parallel environment (4 environment runners each with
    16 vectorized environments)
- Uses gradient clipping by global norm for training stability
- Expects to reach reward of 300.0 within 1 million timesteps

How to run this script
----------------------
`python mujoco_ppo.py --env=Humanoid-v4`

Use the `--env` flag to specify different MuJoCo environments (e.g., HalfCheetah-v4,
Hopper-v4, Walker2d-v4, Ant-v4).

Prerequisites (Ubuntu):
```
sudo apt-get install libosmesa-dev patchelf
python -m pip install "gymnasium[mujoco]"
export MUJOCO_GL=osmesa
```

To run with different configuration:
`python mujoco_ppo.py --env=HalfCheetah-v4 --stop-reward=2000.0`

To scale up with distributed learning using multiple learners and env-runners:
`python mujoco_ppo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python mujoco_ppo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
With the default settings on Humanoid-v4, you should expect to reach a reward
of ~300.0 within a million environment timesteps. Different MuJoCo
environments have varying difficulty levels and may require adjusted reward
targets.
"""
from torch.nn.init import orthogonal_

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_iters=1_000,
    default_reward=300.0,
    default_timesteps=1_000_000,
)
parser.set_defaults(
    env="Humanoid-v4",
    num_env_runners=4,
    num_envs_per_env_runner=16,
    num_learners=1,
)
args = parser.parse_args()


config = (
    PPOConfig()
    .environment(env=args.env)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
        rollout_fragment_length=32,
        env_to_module_connector=lambda env: MeanStdFilter(),
    )
    .learners(
        num_learners=args.num_learners,
    )
    .training(
        train_batch_size_per_learner=2048,
        minibatch_size=512,
        num_epochs=10,
        lr=0.0003,
        vf_loss_coeff=1.0,
        entropy_coeff=[
            [0, 0.01],
            [2_000_000, 0.0],
        ],
        gamma=0.99,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.2,
        vf_clip_param=10.0,
        grad_clip=50.0,
        grad_clip_by="global_norm",
    )
    .rl_module(
        model_config=DefaultModelConfig(
            vf_share_layers=False,
            fcnet_hiddens=[256, 256],
            fcnet_kernel_initializer=orthogonal_,
            fcnet_activation="tanh",
            head_fcnet_hiddens=[256],
            head_fcnet_activation=None,
        )
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
