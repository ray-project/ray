import argparse

from rllib_alpha_star.alpha_star import AlphaStar, AlphaStarConfig

import ray
from ray import air, tune
from ray.rllib.utils.test_utils import check_learning_achieved


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-as-test", action="store_true", default=False)
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()
    ray.init()

    config = (
        AlphaStarConfig()
        .rollouts(
            num_rollout_workers=2,
            num_envs_per_worker=5,
            observation_filter="MeanStdFilter",
        )
        .resources(num_gpus=1, _fake_gpus=True)
        .environment(
            "ray.rllib.examples.env.multi_agent.MultiAgentCartPole",
            env_config={"config": {"num_agents": 2}},
        )
        .training(
            gamma=0.95,
            num_sgd_iter=1,
            vf_loss_coeff=0.005,
            vtrace=True,
            model={
                "fcnet_hiddens": [32],
                "fcnet_activation": "linear",
                "vf_share_layers": True,
            },
            replay_buffer_replay_ratio=0.0,
            league_builder_config={
                "type": "rllib_alpha_star.alpha_star.league_builder.NoLeagueBuilder"
            },
        )
        .multi_agent(
            policies=["p0", "p1"],
            policy_mapping_fn={
                "type": "ray.rllib.examples.multi_agent_and_self_play.policy_mapping_fn.PolicyMappingFn"  # noqa
            },
        )
        .debugging(seed=0)
    )

    num_iterations = 100
    stop_reward = 300

    tuner = tune.Tuner(
        AlphaStar,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"episode_reward_mean": stop_reward, "timesteps_total": 200000},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
    if args.run_as_test:
        check_learning_achieved(results, stop_reward)
