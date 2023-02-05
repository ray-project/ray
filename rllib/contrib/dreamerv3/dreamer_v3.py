import logging
from typing import Any

import numpy as np
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.execution.common import (STEPS_SAMPLED_COUNTER,
                                        _get_shared_metrics)
from ray.rllib.policy.sample_batch import (DEFAULT_POLICY_ID)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import (NUM_AGENT_STEPS_SAMPLED,
                                     NUM_ENV_STEPS_SAMPLED)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)

ActFunc = Any

EPS = 1e-8


def _postprocess_gif(gif: np.ndarray):
    """Process provided gif to a format that can be logged to Tensorboard."""
    gif = np.clip(255 * gif, 0, 255).astype(np.uint8)
    B, T, C, H, W = gif.shape
    frames = gif.transpose((1, 2, 3, 0, 4)).reshape((1, T, C, H, B * W))
    return frames


class DreamerIteration:
    def __init__(
            self, worker, episode_buffer, dreamer_train_iters, batch_size, act_repeat
    ):
        self.worker = worker
        self.episode_buffer = episode_buffer
        self.dreamer_train_iters = dreamer_train_iters
        self.repeat = act_repeat
        self.batch_size = batch_size

    def __call__(self, samples):

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            # Dreamer training loop.
            for n in range(self.dreamer_train_iters):
                print(f"sub-iteration={n}/{self.dreamer_train_iters}")
                batch = self.episode_buffer.sample(self.batch_size)
                fetches = self.worker.learn_on_batch(batch)
        else:
            fetches = {}

        # Custom Logging
        policy_fetches = fetches[DEFAULT_POLICY_ID]["learner_stats"]
        if "log_gif" in policy_fetches:
            gif = policy_fetches["log_gif"]
            policy_fetches["log_gif"] = self.postprocess_gif(gif)

        # Metrics Calculation
        metrics = _get_shared_metrics()
        metrics.info[LEARNER_INFO] = fetches
        metrics.counters[STEPS_SAMPLED_COUNTER] = self.episode_buffer.timesteps
        metrics.counters[STEPS_SAMPLED_COUNTER] *= self.repeat
        res = collect_metrics(local_worker=self.worker)
        res["info"] = metrics.info
        res["info"].update(metrics.counters)
        res["timesteps_total"] = metrics.counters[STEPS_SAMPLED_COUNTER]

        self.episode_buffer.add(samples)
        return res

    def postprocess_gif(self, gif: np.ndarray):
        return _postprocess_gif(gif=gif)
