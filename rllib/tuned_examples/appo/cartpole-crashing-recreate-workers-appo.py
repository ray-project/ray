"""
Tests, whether APPO can learn in a fault tolerant fashion.

Workers will be configured to automatically get recreated upon failures (here: within
the environment).
The environment we use here is configured to crash with a certain probability on each
`step()` and/or `reset()` call.
"""
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.env.cartpole_crashing import CartPoleCrashing


stop = {
    "evaluation/sampler_results/episode_reward_mean": 150.0,
    "num_env_steps_sampled": 150000,
}

config = (
    APPOConfig()
    .environment(
        CartPoleCrashing,
        env_config={
            "config": {
                # Crash roughly every 300 ts. This should be ok to measure 180.0
                # reward (episodes are 200 ts long).
                "p_crash": 0.0025,  # prob to crash during step()
                "p_crash_reset": 0.01,  # prob to crash during reset()
            },
        },
        # Disable env checking. Env checker doesn't handle Exceptions from
        # user envs, and will crash rollout worker.
        disable_env_checking=True,
    )
    .rollouts(
        num_rollout_workers=10,
        num_envs_per_worker=5,
    )
    # Switch on resiliency for failed sub environments (within a vectorized stack).
    .fault_tolerance(
        recreate_failed_workers=True,
    )
    .evaluation(
        evaluation_num_workers=2,
        evaluation_interval=1,
        evaluation_duration=20,
        evaluation_duration_unit="episodes",
        evaluation_parallel_to_training=True,
        enable_async_evaluation=True,
        evaluation_config=APPOConfig.overrides(
            explore=False,
            env_config={
                "config": {
                    # Make eval workers solid.
                    # This test is to prove that we can learn with crashing envs,
                    # not evaluate with crashing envs.
                    "p_crash": 0.0,
                    "p_crash_reset": 0.0,
                    "init_time_s": 0.0,
                },
            },
        ),
    )
)
