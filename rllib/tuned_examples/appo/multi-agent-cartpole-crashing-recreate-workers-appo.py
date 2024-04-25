"""
Tests, whether APPO can learn in a fault-tolerant fashion in a
multi-agent setting.

Workers will be configured to automatically get recreated upon failures (here: within
the environment).
The environment we use here is configured to crash with a certain probability on each
`step()` and/or `reset()` call.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.envs.classes.cartpole_crashing import MultiAgentCartPoleCrashing
from ray import tune

tune.register_env("ma_env", lambda cfg: MultiAgentCartPoleCrashing(cfg))

stop = {
    "evaluation/sampler_results/episode_reward_mean": 800.0,
    "num_env_steps_sampled": 250000,
}

config = (
    APPOConfig()
    .environment(
        "ma_env",
        env_config={
            "num_agents": 2,
            # Crash roughly every 300 ts. This should be ok to measure 180.0
            # reward (episodes are 200 ts long).
            "p_crash": 0.0005,  # prob to crash during step()
            "p_crash_reset": 0.005,  # prob to crash during reset()
        },
    )
    .env_runners(
        num_rollout_workers=4,
        num_envs_per_env_runner=1,
    )
    # Switch on resiliency (recreate any failed worker).
    .fault_tolerance(
        recreate_failed_env_runners=True,
    )
    .evaluation(
        evaluation_num_env_runners=1,
        evaluation_interval=1,
        evaluation_duration=25,
        evaluation_duration_unit="episodes",
        evaluation_parallel_to_training=True,
        evaluation_config=APPOConfig.overrides(
            explore=False,
            env_config={
                # Make eval workers solid.
                # This test is to prove that we can learn with crashing envs,
                # not evaluate with crashing envs.
                "p_crash": 0.0,
                "p_crash_reset": 0.0,
                "init_time_s": 0.0,
            },
        ),
    )
)
