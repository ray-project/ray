# @OldAPIStack
"""
Tests, whether APPO can learn in a fault-tolerant fashion.

Workers will be configured to automatically get recreated upon failures (here: within
the environment).
The environment we use here is configured to crash with a certain probability on each
`step()` and/or `reset()` call.
"""
from ray import tune
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.envs.classes.cartpole_crashing import CartPoleCrashing
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)

tune.register_env("env", lambda cfg: CartPoleCrashing(cfg))


stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 400.0,
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 250000,
}

config = (
    APPOConfig()
    .api_stack(
        enable_rl_module_and_learner=False,
        enable_env_runner_and_connector_v2=False,
    )
    .environment(
        "env",
        env_config={
            # Crash roughly every 500 ts.
            "p_crash": 0.0005,  # prob to crash during step()
            "p_crash_reset": 0.005,  # prob to crash during reset()
            "crash_on_worker_indices": [1, 2],
        },
    )
    .env_runners(
        num_env_runners=3,
        num_envs_per_env_runner=1,
    )
    # Switch on resiliency (recreate any failed worker).
    .fault_tolerance(
        restart_failed_env_runners=True,
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
