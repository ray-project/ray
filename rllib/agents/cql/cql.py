"""CQL (derived from SAC).
"""
from typing import Optional, Type, List

from ray.rllib.agents.sac.sac import SACTrainer, \
    DEFAULT_CONFIG as SAC_CONFIG
from ray.rllib.agents.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.policy.policy import Policy
from ray.rllib.utils import merge_dicts

from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.execution.replay_ops import Replay
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainOneStep, UpdateTargetNetwork
from ray.rllib.policy.policy import LEARNER_STATS_KEY
from ray.rllib.agents.dqn.dqn import calculate_rr_weights

# yapf: disable
# __sphinx_doc_begin__
CQL_DEFAULT_CONFIG = merge_dicts(
    SAC_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Offline RL does not need IS estimators
        "input_evaluation": [],
        # Number of iterations with Behavior Cloning Pretraining
        "bc_iters": 20000,
        # CQL Loss Temperature
        "temperature": 1.0,
        # Num Actions to sample for CQL Loss
        "num_actions": 10,
        # Whether to use the Langrangian for Alpha Prime (in CQL Loss)
        "lagrangian": False,
        # Lagrangian Threshold
        "lagrangian_thresh": 5.0,
        # Min Q Weight multiplier
        "min_q_weight": 5.0,
        # Replay Buffer should be size of offline dataset
        "buffer_size": 1000000,
    })
# __sphinx_doc_end__
# yapf: enable


def validate_config(config: TrainerConfigDict):
    if config["num_gpus"] > 1:
        raise ValueError("`num_gpus` > 1 not yet supported for CQL!")
    if config["framework"] == "tf":
        raise ValueError("Tensorflow CQL not implemented yet!")


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    if config["framework"] == "torch":
        return CQLTorchPolicy


replay_buffer = None


class NoOpReplayBuffer:
    def __init__(self,
                 *,
                 local_buffer: LocalReplayBuffer = None,
                 actors: List["ActorHandle"] = None):
        return

    def __call__(self, batch):
        return batch


def execution_plan(workers, config):
    if config.get("prioritized_replay"):
        prio_args = {
            "prioritized_replay_alpha": config["prioritized_replay_alpha"],
            "prioritized_replay_beta": config["prioritized_replay_beta"],
            "prioritized_replay_eps": config["prioritized_replay_eps"],
        }
    else:
        prio_args = {}

    local_replay_buffer = LocalReplayBuffer(
        num_shards=1,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        replay_batch_size=config["train_batch_size"],
        replay_mode=config["multiagent"]["replay_mode"],
        replay_sequence_length=config.get("replay_sequence_length", 1),
        **prio_args)

    global replay_buffer
    replay_buffer = local_replay_buffer

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    store_op = rollouts.for_each(
        NoOpReplayBuffer(local_buffer=local_replay_buffer))

    def update_prio(item):
        samples, info_dict = item
        if config.get("prioritized_replay"):
            prio_dict = {}
            for policy_id, info in info_dict.items():
                td_error = info.get("td_error",
                                    info[LEARNER_STATS_KEY].get("td_error"))
                prio_dict[policy_id] = (samples.policy_batches[policy_id]
                                        .data.get("batch_indexes"), td_error)
            local_replay_buffer.update_priorities(prio_dict)
        return info_dict

    post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)
    replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(lambda x: post_fn(x, workers, config)) \
        .for_each(TrainOneStep(workers)) \
        .for_each(update_prio) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"]))

    train_op = Concurrently(
        [store_op, replay_op],
        mode="round_robin",
        output_indexes=[1],
        round_robin_weights=calculate_rr_weights(config))

    return StandardMetricsReporting(train_op, workers, config)


def after_init(trainer):
    # Add the entire dataset to Replay Buffer (global variable)
    global replay_buffer
    local_worker = trainer.workers.local_worker
    if "d4rl" in trainer.config["input"]:
        dataset = local_worker().input_reader.dataset
        replay_buffer.add_batch(dataset)


CQLTrainer = SACTrainer.with_updates(
    name="CQL",
    default_config=CQL_DEFAULT_CONFIG,
    validate_config=validate_config,
    default_policy=CQLTorchPolicy,
    get_policy_class=get_policy_class,
    after_init=after_init,
    execution_plan=execution_plan,
)
