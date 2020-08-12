import logging

import random
import numpy as np
import ray
from ray.rllib.utils.sgd import standardized
from ray.rllib.agents import with_common_config
from ray.rllib.agents.dreamer.dreamer_torch_policy import DreamerTorchPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER, LEARNER_INFO, _get_shared_metrics
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.execution.metric_ops import CollectMetrics
from ray.util.iter import from_actors
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.torch_ops import convert_to_torch_tensor
from ray.rllib.evaluation.metrics import collect_episodes
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.agents.dreamer.dreamer_model import DreamerModel
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.util.iter_metrics import SharedMetrics
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # PlaNET Model LR
    "model_lr": 6e-4,
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
    # Training iterations per data collection from real env
    "dreamer_train_iters": 100,
    # If specified, clip the global norm of gradients by this amount
    "horizon": 1000,
    # Number of episodes to sample for Loss Calculation
    "batch_size": 50,
    # Length of each episode to sample for Loss Calculation
    "batch_length": 50,
    # Imagination Horizon
    "imagine_horizon": 15,
    # Free Nats
    "free_nats": 3.0,
    # KL Coeff for the Model Loss
    "kl_coeff": 1.0,
    # Action Repeat (Dreamer does best at 2)
    "action_repeat": 2,
    # Number of Workers should be 0
    "num_workers": 0,
    # Prefill Timesteps
    "prefill_timesteps": 5000,
    # This should be kept at 1 to preserve sample efficiency
    "num_envs_per_worker": 1,
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
        # Action Std
        "hidden_size": 400,
        "action_init_std": 5.0,
    },
})
# __sphinx_doc_end__
# yapf: enable

class EpisodicBuffer(object):

  def __init__(self, max_length=5000, length=50):
    # Stores all episodes into a list: List[SampleBatchType]
    self.episodes = []
    self.max_length = max_length
    self.timesteps = 0
    self.length = length

  def add(self, batch):
    self.timesteps += batch.count
    # Delete State keys (TD model is RNN but Policy is not)
    batch = {k: v for k,v in batch.data.items() if "state" not in k}
    batch = SampleBatch(batch)
    episodes = batch.split_by_episode()
    self.episodes.extend(episodes)
    if len(self.episodes) > self.max_length:
      delta = len(self.episodes) - self.max_length
      # Drop oldest episodes
      self.episodes = self.episodes[delta:]

  def sample(self, batch_size):
    episodes_buffer = []
    while len(episodes_buffer) < batch_size:
      rand_index = random.randint(0, len(self.episodes)-1)
      episode = self.episodes[rand_index]
      if episode.count < self.length:
        continue
      available = episode.count - self.length
      index = int(random.randint(0, available))
      episodes_buffer.append(episode.slice(index, index + self.length))

    batch = {}
    for k in episodes_buffer[0].keys():
        batch[k] = np.stack([e[k] for e in episodes_buffer], axis=0)

    return SampleBatch(batch)

def total_sampled_timesteps(worker):
    return worker.policy_map[DEFAULT_POLICY_ID].global_timestep
# Similar to Dreamer Execution Plan
def execution_plan(workers, config):
    episode_buffer = EpisodicBuffer(length=config["batch_length"])
    local_worker = workers.local_worker()

    while total_sampled_timesteps(local_worker) < config["prefill_timesteps"]:
        samples = local_worker.sample()
        episode_buffer.add(samples)

    batch_size = config["batch_size"]
    dreamer_train_iters = config["dreamer_train_iters"]
    
    def dreamer_iteration(itr):
        for samples in itr:
            episode_buffer.add(samples)
            for n in range(dreamer_train_iters):
                print(n)
                batch = episode_buffer.sample(batch_size)
                fetches = local_worker.learn_on_batch(batch)

            # Logging
            metrics = _get_shared_metrics()
            metrics.info[LEARNER_INFO] = fetches
            res = collect_metrics(local_worker=local_worker)
            res["info"] = metrics.info
            res["info"].update(metrics.counters)
            metrics.counters[STEPS_SAMPLED_COUNTER] = episode_buffer.timesteps*config["action_repeat"]
            res["timesteps_total"] = metrics.counters[STEPS_SAMPLED_COUNTER]
            yield res

    rollouts = ParallelRollouts(workers)
    rollouts = rollouts.transform(dreamer_iteration)
    return rollouts


def get_policy_class(config):
    return DreamerTorchPolicy


def validate_config(config):
    if config["framework"] != "torch":
        raise ValueError("Dreamer not supported in Tensorflow yet!")
    if config["batch_mode"] != "complete_episodes":
        raise ValueError("truncate_episodes not supported")
    if config["action_repeat"] > 1:
        config["horizon"] = config["horizon"]/config["action_repeat"]



DREAMERTrainer = build_trainer(
    name="Dreamer",
    default_config=DEFAULT_CONFIG,
    default_policy=DreamerTorchPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    validate_config=validate_config)
