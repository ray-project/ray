from ray.tune.registry import register_env
from ray.rllib.connectors.module_to_env.action_masking_off_policy import (
    ActionMaskingOffPolicy
)
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.envs.classes.cartpole_with_action_masking import (
    CartPoleWithActionMasking
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=400.0)
# Use DQN by default (PPO is on-policy and won't work with this simple, connector-based
# setup).
parser.set_defaults(
    enable_new_api_stack=True,
    algo="DQN",
)


if __name__ == "__main__":
    args = parser.parse_args()

    allowed_actions_key = "allowed_actions"
    allowed_actions_location = "infos"

    # Register our environment with tune.
    if args.num_agents > 0:
        raise ValueError("`num_agents` > 0 not supported yet for this example!")
    else:
        register_env("env", lambda cfg: CartPoleWithActionMasking(cfg))

    # Define the AlgorithmConfig used.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env", env_config={
            "num_actions": 4,
            "allowed_actions_key": allowed_actions_key,
            "allowed_actions_location": allowed_actions_location,
        })
        .env_runners(
            module_to_env_connector=lambda env: ActionMaskingOffPolicy(
                allowed_actions_key=allowed_actions_key,
                allowed_actions_location=allowed_actions_location,
            ),
        )
        .training(gamma=0.99, lr=0.0003)
        .rl_module(
            model_config_dict=dict(
                {
                    "fcnet_hiddens": [32],
                    "fcnet_activation": "linear",
                    "vf_share_layers": True,
                },
                **(
                    {}
                    if not args.enable_new_api_stack
                    else {"uses_new_env_runners": True}
                ),
            ),
        )
    )

    # Run everything as configured.
    run_rllib_example_script_experiment(config, args)
