"""Example of running a single-agent pre-training followed with a multi-agent training.

This examples `num_agents` agents each of them with its own `RLModule` that defines its
policy. The first agent is pre-trained using a single-agent PPO algorithm. All agents
are trained together in the main training run using a multi-agent PPO algorithm where
the pre-trained module is used for the first agent.

The environment is MultiAgentCartPole, in which there are n agents both policies.

How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=2`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that the single-agent policy is first trained until
the specified `--stop-reward-pretraining` value. For example, with the command line:
`--enable-new-api-stack --num-agents=2 --stop-reward-pretraining=250.0
--stop-reward=250.0 --stop-iters=3 --as-test`, you should get something like:
+-----------------------+------------+------+----------------+---------------------+
| Trial name            | status     | iter | total time (s) | episode_return_mean |
|                       |            |      |                |                     |
|-----------------------+------------+------+----------------+---------------------+
| PPO_CartPole-v1_00000 | TERMINATED |   16 |        25.6009 |               256.2 |
+-----------------------+------------+------+----------------+---------------------+

Then, in the second experiment, where we run in a multi-agent setup with two policies
("p0" from the single-agent checkpoint and "p1" randomly initialized), you can see that
only "p0" immediately (after 1 iteration) reaches the same episode return as at the end
of pretraining:
+----------------------------+------------+--------+------------------+------+
| Trial name                 | status     |   iter |   total time (s) |   ts |
|----------------------------+------------+--------+------------------+------+
| PPO_multi-cart_6274d_00000 | TERMINATED |      1 |          2.71681 | 4000 |
+----------------------------+------------+--------+------------------+------+
+-------------------+-------------+-------------+
|   combined return |   return p0 |   return p1 |
|-------------------+-------------|-------------+
|           451.625 |     433.125 |        18.5 |
+-------------------+-------------+-------------+
"""
from pathlib import Path

import gymnasium as gym

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_LEARNER_GROUP,
    COMPONENT_RL_MODULE,
    DEFAULT_MODULE_ID,
)
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env

# Read in common example script command line arguments.
parser = add_rllib_example_script_args(
    # Use less training steps for the main training run.
    default_timesteps=50000,
    default_reward=200.0,
    default_iters=20,
)
parser.set_defaults(
    checkpoint_freq=1,
    checkpoint_at_end=True,
)
parser.add_argument(
    "--stop-reward-pretraining",
    type=float,
    default=250.0,
    help="The min. episode return to reach during pre-training.",
)


if __name__ == "__main__":

    # Parse the command line arguments.
    args = parser.parse_args()

    # Ensure that the user has set the number of agents.
    if args.num_agents == 0:
        raise ValueError(
            "This pre-training example script requires at least 1 agent. "
            "Try setting the command line argument `--num-agents` to the "
            "number of agents you want to use."
        )

    # Store the user's stopping criteria for the later training run.
    stop_iters = args.stop_iters
    stop_timesteps = args.stop_timesteps
    stop_reward = args.stop_reward
    num_agents = args.num_agents
    as_test = args.as_test

    # Override these criteria for the pre-training run.
    args.stop_iters = 10000
    args.stop_timesteps = 100000000
    args.stop_reward = args.stop_reward_pretraining
    args.num_agents = 0
    args.as_test = False

    # Define out pre-training single-agent algorithm. We will use the same module
    # configuration for the pre-training and the training.
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .rl_module(
            # Use a different number of hidden units for the pre-trained module.
            model_config=DefaultModelConfig(fcnet_hiddens=[64]),
        )
    )

    # Run the pre-training.
    results = run_rllib_example_script_experiment(config, args, keep_ray_up=True)
    # Get the checkpoint path.
    module_chkpt_path = (
        Path(results.get_best_result().checkpoint.path)
        / COMPONENT_LEARNER_GROUP
        / COMPONENT_LEARNER
        / COMPONENT_RL_MODULE
        / DEFAULT_MODULE_ID
    )
    assert module_chkpt_path.is_dir()

    # Restore the user's stopping criteria for the training run.
    args.stop_iters = stop_iters
    args.stop_timesteps = stop_timesteps
    args.stop_reward = stop_reward
    args.num_agents = num_agents
    args.as_test = as_test

    # Create a new MultiRLModule using the pre-trained module for policy 0.
    env = gym.make("CartPole-v1")
    module_specs = {}
    module_class = PPOTorchRLModule
    for i in range(args.num_agents):
        module_specs[f"p{i}"] = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config=DefaultModelConfig(fcnet_hiddens=[32]),
            catalog_class=PPOCatalog,
        )

    # Swap in the pre-trained module for policy 0.
    module_specs["p0"] = RLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config=DefaultModelConfig(fcnet_hiddens=[64]),
        catalog_class=PPOCatalog,
        # Note, we load here the module directly from the checkpoint.
        load_state_path=module_chkpt_path,
    )
    multi_rl_module_spec = MultiRLModuleSpec(rl_module_specs=module_specs)

    # Register our environment with tune if we use multiple agents.
    register_env(
        "multi-cart",
        lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
    )

    # Configure the main (multi-agent) training run.
    config = (
        PPOConfig()
        .environment("multi-cart")
        .multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, eps, **kw: f"p{aid}",
        )
        .rl_module(rl_module_spec=multi_rl_module_spec)
    )

    # Run the main training run.
    run_rllib_example_script_experiment(config, args)
