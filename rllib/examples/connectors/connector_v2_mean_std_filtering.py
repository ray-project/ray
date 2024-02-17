import functools

from ray.rllib.connectors.env_to_module.mean_std_filter import MeanStdFilter
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

torch, _ = try_import_torch()

parser = add_rllib_example_script_args(
    default_iters=500,
    default_timesteps=500000,
    default_reward=-300.0,
)


if __name__ == "__main__":
    args = parser.parse_args()

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .framework(args.framework)
        .environment("Pendulum-v1")
        # Use new API stack ...
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .rollouts(
            # ... new EnvRunner and our frame stacking env-to-module connector.
            env_runner_cls=SingleAgentEnvRunner,
            num_rollout_workers=args.num_env_runners,
            num_envs_per_worker=20,
            # Define our custom connector pipeline.
            # Alternatively, return a list of n ConnectorV2 pieces (which will then be
            # included in an automatically generated EnvToModulePipeline or return a
            # EnvToModulePipeline directly.
            env_to_module_connector=lambda env: (
                MeanStdFilter(
                    input_observation_space=env.single_observation_space,
                    input_action_space=env.single_action_space,
                )
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
                    "fcnet_bias_initializer": (
                        functools.partial(torch.nn.init.constant_, val=0.0)
                    ),
                },
                **({"uses_new_env_runners": True} if args.enable_new_api_stack else {})
            ),
        )
    )

    run_rllib_example_script_experiment(config, args)
