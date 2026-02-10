"""Tests integration between Ray Tune and RLlib

Runs a learning test: APPO with Atari Pong
"""

import json
import os
import time

import gymnasium as gym

import ray
from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN
from ray.tune import CLIReporter, RunConfig, Tuner
from ray.tune.registry import register_env


def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(id="ale_py:ALE/Pong-v5", **cfg, **{"render_mode": "rgb_array"}),
        dim=64,
        framestack=None,
    )


register_env(name="env", env_creator=_env_creator)

stop = {"training_iteration": 50}
config = (
    APPOConfig()
    .environment(
        env="env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(
        env_to_module_connector=_make_env_to_module_connector,
        num_env_runners=4,
        num_envs_per_env_runner=5,
        rollout_fragment_length=50,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size=1000,
        vf_loss_coeff=1.0,
        clip_param=0.3,
        grad_clip=10,
        vtrace=True,
        use_kl_loss=False,
    )
    .learners(
        num_learners=1,
        num_gpus_per_learner=1,
    )
)


if __name__ == "__main__":
    ray.init(ignore_reinit_error=True)

    start_time = time.time()
    results = Tuner(
        trainable=config.algo_class,
        param_space=config,
        run_config=RunConfig(
            stop=stop,
            verbose=1,
            progress_reporter=CLIReporter(
                metric_columns={
                    "training_iteration": "training_iteration",
                    f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": f"{EPISODE_RETURN_MEAN}",
                },
                max_report_frequency=30,
            ),
        ),
    ).fit()

    exp_analysis = results._experiment_analysis
    end_time = time.time()

    result = {
        "time_taken": end_time - start_time,
        "trial_state": [trial.status for trial in exp_analysis.trials],
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Ok.")
