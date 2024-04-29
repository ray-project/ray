import functools

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    PrevActionsPrevRewardsConnector,
    WriteObservationsToEpisodes,
)
from ray.rllib.examples.envs.classes.stateless_cartpole import StatelessCartPole
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentStatelessCartPole
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune import register_env

torch, nn = try_import_torch()


parser = add_rllib_example_script_args(
    default_reward=200.0, default_timesteps=1000000, default_iters=2000
)
parser.add_argument("--n-prev-rewards", type=int, default=1)
parser.add_argument("--n-prev-actions", type=int, default=1)


if __name__ == "__main__":
    args = parser.parse_args()

    # Define our custom connector pipelines.
    def _env_to_module(env):
        # Create the env-to-module connector pipeline.
        return [
            AddObservationsFromEpisodesToBatch(),
            PrevActionsPrevRewardsConnector(
                multi_agent=args.num_agents > 0,
                n_prev_rewards=args.n_prev_rewards,
                n_prev_actions=args.n_prev_actions,
            ),
            FlattenObservations(multi_agent=args.num_agents > 0),
            WriteObservationsToEpisodes(),
        ]

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentStatelessCartPole(
                config={"num_agents": args.num_agents}
            ),
        )
    else:
        register_env("env", lambda _: StatelessCartPole())

    config = (
        PPOConfig()
        .environment("env")
        .env_runners(env_to_module_connector=_env_to_module)
        .training(
            num_sgd_iter=6,
            lr=0.0003,
            train_batch_size=4000,
            vf_loss_coeff=0.01,
        )
    )

    if args.enable_new_api_stack:
        config = config.rl_module(
            model_config_dict={
                "use_lstm": True,
                "max_seq_len": 50,
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
                "fcnet_weights_initializer": nn.init.xavier_uniform_,
                "fcnet_bias_initializer": functools.partial(nn.init.constant_, 0.0),
                "uses_new_env_runners": True,
            }
        )
    else:
        config = config.training(
            model=dict(
                {
                    "use_lstm": True,
                    "max_seq_len": 50,
                    "fcnet_hiddens": [32],
                    "fcnet_activation": "linear",
                    "vf_share_layers": True,
                    "fcnet_weights_initializer": nn.init.xavier_uniform_,
                    "fcnet_bias_initializer": functools.partial(nn.init.constant_, 0.0),
                }
            )
        )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config = config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    run_rllib_example_script_experiment(config, args)
