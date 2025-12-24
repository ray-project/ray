"""Example showing how to run DreamerV3 on continuous control MuJoCo environments.

DreamerV3 is a model-based reinforcement learning algorithm that learns a world
model to predict future states and rewards, then uses imagination (dreaming) to
train the policy without additional environment interaction. For continuous
control tasks like MuJoCo, DreamerV3's world model learns the physics dynamics,
enabling sample-efficient policy learning.

This example:
    - Runs DreamerV3 on Humanoid-v4 (default) from the MuJoCo suite
    - Uses the "S" model size as recommended in the DreamerV3 paper for
      continuous control tasks with proprioceptive observations
    - Configures a training ratio of 512 (gradient steps per environment step)
    - Provides a configuration suitable for MuJoCo locomotion tasks
    - Expects to achieve good performance within 3 million timesteps

How to run this script
----------------------
`python mujoco_dreamerv3.py [options]`

To run with default settings on Humanoid:
`python mujoco_dreamerv3.py`

To run on a different MuJoCo environment:
`python mujoco_dreamerv3.py --env=HalfCheetah-v4`

To scale up with distributed learning using multiple learners and env-runners:
`python mujoco_dreamerv3.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python mujoco_dreamerv3.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
DreamerV3 should learn effective locomotion policies for MuJoCo tasks. The
world model learns the physics dynamics, allowing the policy to be trained
efficiently through imagination. The "S" model size balances capacity and
training speed for proprioceptive control tasks. Expect rewards around 8000+
on Humanoid-v4 within 3 million timesteps.
"""
from ray.rllib.algorithms.dreamerv3.dreamerv3 import DreamerV3Config
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=8000.0,
    default_timesteps=3_000_000,
)
parser.set_defaults(
    env="Humanoid-v4",
    num_env_runners=4,
)
args = parser.parse_args()


config = (
    DreamerV3Config()
    .environment(args.env)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=4,
    )
    .training(
        model_size="S",
        training_ratio=512,
        batch_size_B=16 * (args.num_learners or 1),
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
