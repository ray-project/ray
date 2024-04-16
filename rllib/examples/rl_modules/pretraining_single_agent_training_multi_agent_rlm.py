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



"""

import gymnasium as gym
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
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
# Instead use mroe for the pre-training run.
parser.add_argument(
    "--stop-iters-pretraining",
    type=int,
    default=200,
    help="The number of iterations to pre-train.",
)
parser.add_argument(
    "--stop-timesteps-pretraining",
    type=int,
    default=5000000,
    help="The number of (environment sampling) timesteps to pre-train.",
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
    checkpoint_at_end = args.checkpoint_at_end
    num_agents = args.num_agents
    # Override these criteria for the pre-training run.
    setattr(args, "stop_iters", args.stop_iters_pretraining)
    setattr(args, "stop_timesteps", args.stop_timesteps_pretraining)
    setattr(args, "checkpoint_at_end", True)
    setattr(args, "num_agents", 0)

    # Define out pre-training single-agent algorithm. We will use the same module
    # configuration for the pre-training and the training.
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .rl_module(
            # Use a different number of hidden units for the pre-trained module.
            model_config_dict={"fcnet_hiddens": [64]},
        )
    )

    # Run the pre-training.
    results = run_rllib_example_script_experiment(config, args)
    # Get the checkpoint path.
    module_chkpt_path = results.get_best_result().checkpoint.path

    # Create a new MARL Module using the pre-trained module for policy 0.
    env = gym.make("CartPole-v1")
    module_specs = {}
    module_class = PPOTorchRLModule
    for i in range(args.num_agents):
        module_specs[f"policy_{i}"] = SingleAgentRLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict={"fcnet_hiddens": [32]},
            catalog_class=PPOCatalog,
        )

    # Swap in the pre-trained module for policy 0.
    module_specs["policy_0"] = SingleAgentRLModuleSpec(
        module_class=PPOTorchRLModule,
        observation_space=env.observation_space,
        action_space=env.action_space,
        model_config_dict={"fcnet_hiddens": [64]},
        catalog_class=PPOCatalog,
        # Note, we load here the module directly from the checkpoint.
        load_state_path=module_chkpt_path,
    )
    marl_module_spec = MultiAgentRLModuleSpec(module_specs=module_specs)

    # Register our environment with tune if we use multiple agents.
    register_env(
        "multi-agent-carpole-env",
        lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
    )

    # Configure the main (multi-agent) training run.
    config = (
        PPOConfig()
        .environment(
            "multi-agent-carpole-env" if args.num_agents > 0 else "CartPole-v1"
        )
        .rl_module(rl_module_spec=marl_module_spec)
    )

    # Restore the user's stopping criteria for the training run.
    setattr(args, "stop_iters", stop_iters)
    setattr(args, "stop_timesteps", stop_timesteps)
    setattr(args, "checkpoint_at_end", checkpoint_at_end)
    setattr(args, "num_agents", num_agents)

    # Run the main training run.
    run_rllib_example_script_experiment(config, args)
