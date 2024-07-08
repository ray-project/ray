# @OldAPIStack
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
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    EVALUATION_RESULTS,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray import tune

tune.register_env("ma_env", lambda cfg: MultiAgentCartPoleCrashing(cfg))

stop = {
    f"{EVALUATION_RESULTS}/{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": 800.0,
    f"{NUM_ENV_STEPS_SAMPLED_LIFETIME}": 250000,
}

config = (
    APPOConfig()
    .environment(
        "ma_env",
        env_config={
            "num_agents": 2,
            # Crash roughly every 300 ts. This should be ok to measure 180.0
            # reward (episodes are 200 ts long).
            "p_crash": 0.00005,  # prob to crash during step()
            "p_crash_reset": 0.0005,  # prob to crash during reset()
            "init_time_s": 2.0,
            "p_stall": 0.001,  # prob to stall during step()
            "p_stall_reset": 0.001,  # prob to stall during reset()
            "stall_time_sec": (2, 5),  # stall between 2 and 10sec.
            "stall_on_worker_indices": [2, 3],
        },
    )
    .env_runners(
        num_env_runners=3,
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
                "p_stall": 0.0,
                "p_stall_reset": 0.0,
            },
        ),
    )
)
