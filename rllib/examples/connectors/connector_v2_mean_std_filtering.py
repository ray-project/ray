from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentPendulum
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=-300.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentPendulum(config={"num_agents": args.num_agents}),
        )

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .framework(args.framework)
        .environment("env" if args.num_agents > 0 else "Pendulum-v1")
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .rollouts(
            # Set up the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None
                if not args.enable_new_api_stack
                else SingleAgentEnvRunner
                if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
            num_rollout_workers=args.num_env_runners,
            # TODO (sven): MAEnvRunner does not support vectorized envs yet
            #  due to gym's env checkers and non-compatability with RLlib's
            #  MultiAgentEnv API.
            num_envs_per_worker=1 if args.num_agents > 0 else 20,
            # Define a single connector piece to be prepended to the env-to-module
            # connector pipeline.
            # Alternatively, return a list of n ConnectorV2 pieces (which will then be
            # included in an automatically generated EnvToModulePipeline or return a
            # EnvToModulePipeline directly.
            env_to_module_connector=(
                lambda env: MeanStdFilter(multi_agent=args.num_agents > 0)
            ),
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=1 if args.num_gpus else 0,
            num_cpus_for_local_worker=1,
        )
        .training(
            train_batch_size_per_learner=512,
            mini_batch_size_per_learner=64,
            gamma=0.95,
            # Linearly adjust learning rate based on number of GPUs.
            lr=0.0003 * (args.num_gpus or 1),
            lambda_=0.1,
            vf_clip_param=10.0,
            vf_loss_coeff=0.01,
            model=dict(
                {
                    "fcnet_activation": "relu",
                    "fcnet_weights_initializer": torch.nn.init.xavier_uniform_,
                    "fcnet_bias_initializer": torch.nn.init.constant_,
                    "fcnet_bias_initializer_config": {"val": 0.0},
                },
                **({"uses_new_env_runners": True} if args.enable_new_api_stack else {}),
            ),
        )
        # .evaluation(
        #    evaluation_num_workers=1,
        #    evaluation_parallel_to_training=True,
        #    evaluation_interval=1,
        #    evaluation_duration=10,
        #    evaluation_duration_unit="episodes",
        #    evaluation_config={"explore": False},
        # )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(config, args)
