import logging

import random
import numpy as np

from ray.rllib.agents import with_common_config
from ray.rllib.agents.dreamer.dreamer_torch_policy import DreamerTorchPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    _get_shared_metrics
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.agents.dreamer.dreamer_model import DreamerModel
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.typing import SampleBatchType, TrainerConfigDict

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # PlaNET Model LR
    "td_model_lr": 6e-4,
    # Actor LR
    "actor_lr": 8e-5,
    # Critic LR
    "critic_lr": 8e-5,
    # Grad Clipping
    "grad_clip": 100.0,
    # Discount
    "discount": 0.99,
    # Lambda
    "lambda": 0.95,
    # Clipping is done inherently via policy tanh.
    "clip_actions": False,
    # Training iterations per data collection from real env
    "dreamer_train_iters": 100,
    # Horizon for Enviornment (1000 for Mujoco/DMC)
    "horizon": 1000,
    # Number of episodes to sample for Loss Calculation
    "batch_size": 50,
    # Length of each episode to sample for Loss Calculation
    "batch_length": 50,
    # Imagination Horizon for Training Actor and Critic
    "imagine_horizon": 15,
    # Free Nats
    "free_nats": 3.0,
    # KL Coeff for the Model Loss
    "kl_coeff": 1.0,
    # Distributed Dreamer not implemented yet
    "num_workers": 0,
    # Prefill Timesteps
    "prefill_timesteps": 5000,
    # This should be kept at 1 to preserve sample efficiency
    "num_envs_per_worker": 1,
    # Exploration Gaussian
    "explore_noise": 0.3,
    # Batch mode
    "batch_mode": "complete_episodes",
    # Custom Model
    "dreamer_model": {
        "custom_model": DreamerModel,
        # RSSM/PlaNET parameters
        "deter_size": 200,
        "stoch_size": 30,
        # CNN Decoder Encoder
        "depth_size": 32,
        # General Network Parameters
        "hidden_size": 400,
        # Action STD
        "action_init_std": 5.0,
    },

    "env_config": {
        # Repeats action send by policy for frame_skip times in env
        "frame_skip": 2,
    }
})
# __sphinx_doc_end__
# yapf: enable


class EpisodicBuffer(object):
    def __init__(self, max_length: int = 1000, length: int = 50):
        """Data structure that stores episodes and samples chunks
        of size length from episodes

        Args:
            max_length: Maximum episodes it can store
            length: Episode chunking lengh in sample()
        """

        # Stores all episodes into a list: List[SampleBatchType]
        self.episodes = []
        self.max_length = max_length
        self.timesteps = 0
        self.length = length

    def add(self, batch: SampleBatchType):
        """Splits a SampleBatch into episodes and adds episodes
        to the episode buffer

        Args:
            batch: SampleBatch to be added
        """

        self.timesteps += batch.count
        episodes = batch.split_by_episode()
        self.episodes.extend(episodes)

        if len(self.episodes) > self.max_length:
            delta = len(self.episodes) - self.max_length
            # Drop oldest episodes
            self.episodes = self.episodes[delta:]

    def sample(self, batch_size: int):
        """Samples [batch_size, length] from the list of episodes

        Args:
            batch_size: batch_size to be sampled
        """
        episodes_buffer = []
        while len(episodes_buffer) < batch_size:
            rand_index = random.randint(0, len(self.episodes) - 1)
            episode = self.episodes[rand_index]
            if episode.count < self.length:
                continue
            available = episode.count - self.length
            index = int(random.randint(0, available))
            episodes_buffer.append(episode[index:index + self.length])

        return SampleBatch.concat_samples(episodes_buffer)


def total_sampled_timesteps(worker):
    return worker.policy_map[DEFAULT_POLICY_ID].global_timestep


class DreamerIteration:
    def __init__(self, worker, episode_buffer, dreamer_train_iters, batch_size,
                 act_repeat):
        self.worker = worker
        self.episode_buffer = episode_buffer
        self.dreamer_train_iters = dreamer_train_iters
        self.repeat = act_repeat
        self.batch_size = batch_size

    def __call__(self, samples):

        # Dreamer training loop.
        for n in range(self.dreamer_train_iters):
            print(f"sub-iteration={n}/{self.dreamer_train_iters}")
            batch = self.episode_buffer.sample(self.batch_size)
            # if n == self.dreamer_train_iters - 1:
            #     batch["log_gif"] = True
            fetches = self.worker.learn_on_batch(batch)

        # Custom Logging
        policy_fetches = self.policy_stats(fetches)
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
        gif = np.clip(255 * gif, 0, 255).astype(np.uint8)
        B, T, C, H, W = gif.shape
        frames = gif.transpose((1, 2, 3, 0, 4)).reshape((1, T, C, H, B * W))
        return frames

    def policy_stats(self, fetches):
        return fetches[DEFAULT_POLICY_ID]["learner_stats"]


class DREAMERTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        config["action_repeat"] = config["env_config"]["frame_skip"]
        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for Dreamer!")
        if config["framework"] != "torch":
            raise ValueError("Dreamer not supported in Tensorflow yet!")
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("truncate_episodes not supported")
        if config["num_workers"] != 0:
            raise ValueError("Distributed Dreamer not supported yet!")
        if config["clip_actions"]:
            raise ValueError("Clipping is done inherently via policy tanh!")
        if config["action_repeat"] > 1:
            config["horizon"] = config["horizon"] / config["action_repeat"]

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict):
        return DreamerTorchPolicy

    @staticmethod
    @override(Trainer)
    def execution_plan(workers, config, **kwargs):
        assert len(kwargs) == 0, (
            "Dreamer execution_plan does NOT take any additional parameters")

        # Special replay buffer for Dreamer agent.
        episode_buffer = EpisodicBuffer(length=config["batch_length"])

        local_worker = workers.local_worker()

        # Prefill episode buffer with initial exploration (uniform sampling)
        while total_sampled_timesteps(
                local_worker) < config["prefill_timesteps"]:
            samples = local_worker.sample()
            episode_buffer.add(samples)

        batch_size = config["batch_size"]
        dreamer_train_iters = config["dreamer_train_iters"]
        act_repeat = config["action_repeat"]

        rollouts = ParallelRollouts(workers)
        rollouts = rollouts.for_each(
            DreamerIteration(local_worker, episode_buffer, dreamer_train_iters,
                             batch_size, act_repeat))
        return rollouts
