import collections
import copy

import ray
from ray.rllib.agents.dqn.dqn import DQNTrainer, calculate_rr_weights
from ray.rllib.agents.dqn.learner_thread import LearnerThread
from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.execution.common import STEPS_TRAINED_COUNTER, \
    SampleBatchType, _get_shared_metrics, _get_global_vars
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.replay_ops import StoreToReplayBuffer, Replay
from ray.rllib.execution.train_ops import UpdateTargetNetwork
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import ReplayActor
from ray.rllib.utils.actors import create_colocated

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    "num_workers": 10,
    "buffer_size": 1000,
    "train_batch_size": 512,
    "rollout_fragment_length": 10,
    "exploration_config": {"type": "Random"},
    # Ratio of train set size over validation set size for dynamics learning.
    # Will be used to decide, which collected batches will be stored in
    # which replay buffer (2 per worker: train and validation). Training of
    # a dynamics model over some epochs (over the entire training set) stops
    # when the validation performance starts to decrease again.
    "train_vs_validation_ratio": 3.0,
    "dynamics_model": {
        "fcnet_hiddens": [512, 512],
    },

    # TODO: (sven) allow for having a default model config over many
    #  sub-models: e.g. "model": {"ModelA": {[default_config]},
    #  "ModelB": [default_config]}

    #"optimizer": merge_dicts(
    #    DQN_CONFIG["optimizer"], {
    #        "max_weight_sync_delay": 400,
    #        "num_replay_buffer_shards": 4,
    #        "debug": False
    #    }),
    #"n_step": 3,
    #"num_gpus": 1,
    #"num_workers": 32,
    #"buffer_size": 2000000,
    #"learning_starts": 50000,
    #"train_batch_size": 512,
    #"rollout_fragment_length": 50,
    #"target_network_update_freq": 500000,
    #"timesteps_per_iteration": 25000,
    #"exploration_config": {"type": "PerWorkerEpsilonGreedy"},
    #"worker_side_prioritization": True,
    #"min_iter_time_s": 30,
    ## If set, this will fix the ratio of sampled to replayed timesteps.
    ## Otherwise, replay will proceed as fast as possible.
    #"training_intensity": None,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.dyna.dyna_torch_policy import DYNATorchPolicy
        return DYNATorchPolicy
    else:
        raise ValueError("tf not supported yet!")


DYNATrainer = build_trainer(
    name="DYNA",
    default_policy=None,
    get_policy_class=get_policy_class,
    default_config=DEFAULT_CONFIG,
    #validate_config=validate_config,
)
