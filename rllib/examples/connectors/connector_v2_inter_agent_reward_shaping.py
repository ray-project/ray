import os

from ray.tune.registry import register_env
import ray.rllib.examples.connectors.connector_v2_classes as classes
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls


# Read in common example script command line arguments.
parser = add_rllib_example_script_args(default_timesteps=200000, default_reward=400.0)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define env-to-module-connector pipeline for the new stack.
    def _learner_pipeline(input_observation_space, input_action_space):
        return classes.inter_agent_reward_shaping.InterAgentRewardShaping()

    # Register our environment with tune.
    register_env(
        "env",
        lambda _: ???(),
    )

    # Define the AlgorithmConfig used.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack for PPO only.
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment("env")
        .framework(args.framework)
        .rollouts(
            num_rollout_workers=args.num_env_runners,
            # Setup the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None if not args.enable_new_api_stack
                else SingleAgentEnvRunner if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        .training(
            learner_connector=_learner_pipeline,
            gamma=0.99,
            lr=0.0003,
            model=dict({
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
            }, **(
                {} if not args.enable_new_api_stack
                else {"uses_new_env_runners": True}
            )),
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        # Add a simple multi-agent setup.
        .multi_agent(
            policies={"p0", "p1"},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )
    )

    # Fix some PPO-specific settings.
    if args.algo == "PPO":
        config.training(
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(config, args)
