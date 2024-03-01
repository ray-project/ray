from ray.tune.registry import register_env
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.cartpole_with_dict_observation_space import (
    CartPoleWithDictObservationSpace,
)
from ray.rllib.examples.env.multi_agent import (
    MultiAgentCartPoleWithDictObservationSpace,
)
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
    def _env_to_module_pipeline(env):
        return [
            AddObservationsFromEpisodesToBatch(),
            FlattenObservations(multi_agent=args.num_agents > 0),
            WriteObservationsToEpisodes(),
        ]

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPoleWithDictObservationSpace(
                config={"num_agents": args.num_agents}
            ),
        )
    else:
        register_env("env", lambda _: CartPoleWithDictObservationSpace())

    # Define the AlgorithmConfig used.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Use new API stack for PPO only.
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment("env")
        .framework(args.framework)
        .resources(
            num_gpus=args.num_gpus,  # old stack
            num_learner_workers=args.num_gpus,  # new stack
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .rollouts(
            env_to_module_connector=_env_to_module_pipeline,
            num_rollout_workers=args.num_env_runners,
            # Set up the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None
                if not args.enable_new_api_stack
                else SingleAgentEnvRunner
                if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        .training(
            gamma=0.99,
            lr=0.0003,
            model=dict(
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

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Fix some PPO-specific settings.
    if args.algo == "PPO":
        config.training(
            num_sgd_iter=6,
            vf_loss_coeff=0.01,
        )

    # Run everything as configured.
    run_rllib_example_script_experiment(config, args)
