"""Example showing how to run multi-agent APPO on TicTacToe with self-play.

This example demonstrates a multi-agent, self-play setup using APPO (Asynchronous
Proximal Policy Optimization) on the TicTacToe environment. The
setup trains multiple policies simultaneously, where each episode randomly pairs
different policies as opponents, creating a diverse training curriculum.

The key insight here is that by training multiple policies against each other
(rather than having a single policy play against itself), we increase the diversity
of playing styles and reduce the risk of overfitting to a particular opponent
strategy.

This example:
    - configures 5 trainable policies (p0 through p4) plus one non-trainable
    random policy as a baseline
    - uses a `policy_mapping_fn` that randomly assigns policies to agents for
    each episode, ensuring diverse matchups
    - demonstrates the use of `policies_to_train` to exclude the `RandomRLModule`
    from training while still using it for evaluation
    - shows how to set up `MultiRLModuleSpec` for multi-agent configurations

How to run this script
----------------------
`python tictactoe_appo.py [options]`

To run with default settings:
`python tictactoe_appo.py`

To scale up with distributed learning using multiple learners and env-runners:
`python tictactoe_appo.py --num-learners=2 --num-env-runners=8`

To use a GPU-based learner add the number of GPUs per learners:
`python tictactoe_appo.py --num-learners=1 --num-gpus-per-learner=1`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0 --num-learners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.
By setting `--num-learners=0` and `--num-env-runners=0` will make them run locally
instead of remote Ray Actor where breakpoints aren't possible.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key]
 --wandb-project=[some project name]
 --wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
Training will run for 100 thousand timesteps (see: `default_timesteps` in the
code) for p0 (policy 0) to achieve a mean return of XX compared to the
random policy. The number of environment steps can be changed through
`default_timesteps`. The trainable policies should gradually improve their
play quality through self-play, learning both offensive strategies (creating
winning sequences) and defensive strategies (blocking opponent sequences).
Due to the random policy matching, each of the 5 policies may develop slightly different
playing styles. You can monitor the episode reward mean for each policy
separately to track learning progress and compare their relative performance.
"""
import random

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent.tic_tac_toe import TicTacToe
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.examples.utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)

parser = add_rllib_example_script_args(
    default_reward=-0.5,
    default_timesteps=10_000_000,
)
parser.set_defaults(
    num_env_runners=5,
    num_envs_per_env_runner=8,
    num_agents=5,
)
args = parser.parse_args()

config = (
    APPOConfig()
    .environment(TicTacToe)
    .env_runners(
        num_env_runners=args.num_env_runners,
        num_envs_per_env_runner=args.num_envs_per_env_runner,
    )
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        train_batch_size_per_learner=256,
        target_network_update_freq=2,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=1.0,
        entropy_coeff=[[0, 0.01], [300_000, 0.0]],
        broadcast_interval=5,
        # learner_queue_size=1,
        circular_buffer_num_batches=4,
        circular_buffer_iterations_per_batch=2,
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs=(
                {
                    f"p{i}": RLModuleSpec(
                        model_config=DefaultModelConfig(vf_share_layers=True),
                    )
                    for i in range(args.num_agents)
                }
                | {"random": RLModuleSpec(module_class=RandomRLModule)}
            ),
        ),
    )
    .multi_agent(
        policies={f"p{i}" for i in range(args.num_agents)} | {"random"},
        policy_mapping_fn=lambda aid, eps, **kw: (
            random.choice([f"p{i}" for i in range(args.num_agents)] + ["random"])
        ),
        policies_to_train=[f"p{i}" for i in range(args.num_agents)],
    )
)


if __name__ == "__main__":
    run_rllib_example_script_experiment(config, args)
    # TODO: Add custom stop condition for results
