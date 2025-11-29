"""Example showing how to run multi-agent APPO on Connect4 with self-play.

This example demonstrates a multi-agent, self-play setup using APPO (Asynchronous
Proximal Policy Optimization) on the Connect4 environment from PettingZoo. The
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
    - uses a CNN-based model with 4 convolutional layers suitable for processing
    the Connect4 board representation
    - employs the same APPO hyperparameters as the Atari example (aggregator actors,
    circular buffer, entropy schedule)

How to run this script
----------------------
`python [script file name].py [options]`

To run with default settings:
`python [script file name].py`

To adjust the number of learners for distributed training:
`python [script file name].py --num-learners=2 --num-env-runners=8`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`

Results to expect
-----------------
Training will run for approximately 400 iterations or 1 million timesteps.
The trainable policies should gradually improve their play quality through
self-play, learning both offensive strategies (creating winning sequences)
and defensive strategies (blocking opponent sequences). Due to the random
policy matching, each of the 5 policies may develop slightly different
playing styles. You can monitor the episode reward mean for each policy
separately to track learning progress and compare their relative performance.
"""
import random

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent.pettingzoo_connect4 import (
    MultiAgentConnect4,
)
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.utils.test_utils import add_rllib_example_script_args

parser = add_rllib_example_script_args(
    default_reward=0.0,
    default_timesteps=1_000_000,
)
args = parser.parse_args()


NUM_POLICIES = 5
main_spec = RLModuleSpec(
    model_config=DefaultModelConfig(vf_share_layers=True),
)


config = (
    APPOConfig()
    .environment(MultiAgentConnect4)
    .learners(
        num_aggregator_actors_per_learner=2,
    )
    .training(
        train_batch_size_per_learner=500,
        target_network_update_freq=2,
        lr=0.0005 * ((args.num_learners or 1) ** 0.5),
        vf_loss_coeff=1.0,
        entropy_coeff=[[0, 0.01], [3000000, 0.0]],  # <- crucial parameter to finetune
        # Only update connector states and model weights every n training_step calls.
        broadcast_interval=5,
        # learner_queue_size=1,
        circular_buffer_num_batches=4,
        circular_buffer_iterations_per_batch=2,
    )
    .rl_module(
        rl_module_spec=MultiRLModuleSpec(
            rl_module_specs=(
                {f"p{i}": main_spec for i in range(NUM_POLICIES)}
                | {"random": RLModuleSpec(module_class=RandomRLModule)}
            ),
        ),
    )
    .multi_agent(
        policies={f"p{i}" for i in range(NUM_POLICIES)} | {"random"},
        policy_mapping_fn=lambda aid, eps, **kw: (
            random.choice([f"p{i}" for i in range(NUM_POLICIES)] + ["random"])
        ),
        policies_to_train=[f"p{i}" for i in range(NUM_POLICIES)],
    )
)


if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args)
