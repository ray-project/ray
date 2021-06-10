"""CQL (derived from SAC).
"""
import numpy as np
from typing import Optional, Type, List

from ray.actor import ActorHandle
from ray.rllib.agents.cql.cql_tf_policy import CQLTFPolicy
from ray.rllib.agents.cql.cql_torch_policy import CQLTorchPolicy
from ray.rllib.agents.dqn.dqn import calculate_rr_weights
from ray.rllib.agents.sac.sac import SACTrainer, \
    DEFAULT_CONFIG as SAC_CONFIG
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.execution.replay_ops import Replay
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainTFMultiGPU, TrainOneStep, \
    UpdateTargetNetwork
from ray.rllib.offline.shuffled_input import ShuffledInput
from ray.rllib.policy.policy import LEARNER_STATS_KEY, Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.typing import TrainerConfigDict

# yapf: disable
# __sphinx_doc_begin__
CQL_DEFAULT_CONFIG = merge_dicts(
    SAC_CONFIG, {
        # You should override this to point to an offline dataset.
        "input": "sampler",
        # Offline RL does not need IS estimators.
        "input_evaluation": [],
        # Number of iterations with Behavior Cloning Pretraining.
        "bc_iters": 20000,
        # CQL Loss Temperature.
        "temperature": 1.0,
        # Num Actions to sample for CQL Loss.
        "num_actions": 10,
        # Whether to use the Lagrangian for Alpha Prime (in CQL Loss).
        "lagrangian": False,
        # Lagrangian Threshold.
        "lagrangian_thresh": 5.0,
        # Min Q Weight multiplier.
        "min_q_weight": 5.0,
        # Replay Buffer should be size of offline dataset.
        "buffer_size": int(1e6),
    })
# __sphinx_doc_end__
# yapf: enable


def validate_config(config: TrainerConfigDict):
    if config["num_gpus"] > 1:
        raise ValueError("`num_gpus` > 1 not yet supported for CQL!")


replay_buffer = None


class NoOpReplayBuffer:
    def __init__(self,
                 *,
                 local_buffer: LocalReplayBuffer = None,
                 actors: List[ActorHandle] = None):
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
        replay_burn_in=config.get("burn_in", 0),
        replay_zero_init_states=config.get("zero_init_states", True),
        **prio_args)

    global replay_buffer
    replay_buffer = local_replay_buffer

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # NoReplayBuffer ensures that no online data is added
    # The Dataset is added to the Replay Buffer in after_init()
    # method below the execution plan.
    store_op = rollouts.for_each(
        NoOpReplayBuffer(local_buffer=local_replay_buffer))

    def update_prio(item):
        samples, info_dict = item
        if config.get("prioritized_replay"):
            prio_dict = {}
            for policy_id, info in info_dict.items():
                # TODO(sven): This is currently structured differently for
                #  torch/tf. Clean up these results/info dicts across
                #  policies (note: fixing this in torch_policy.py will
                #  break e.g. DDPPO!).
                td_error = info.get("td_error",
                                    info[LEARNER_STATS_KEY].get("td_error"))
                samples.policy_batches[policy_id].set_get_interceptor(None)
                prio_dict[policy_id] = (samples.policy_batches[policy_id]
                                        .get("batch_indexes"), td_error)
            local_replay_buffer.update_priorities(prio_dict)
        return info_dict

    # (2) Read and train on experiences from the replay buffer. Every batch
    # returned from the LocalReplay() iterator is passed to TrainOneStep to
    # take a SGD step, and then we decide whether to update the target network.
    post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)

    if config["simple_optimizer"]:
        train_step_op = TrainOneStep(workers)
    else:
        train_step_op = TrainTFMultiGPU(
            workers=workers,
            sgd_minibatch_size=config["train_batch_size"],
            num_sgd_iter=1,
            num_gpus=config["num_gpus"],
            shuffle_sequences=True,
            _fake_gpus=config["_fake_gpus"],
            framework=config.get("framework"))

    replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(lambda x: post_fn(x, workers, config)) \
        .for_each(train_step_op) \
        .for_each(update_prio) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"]))

    # Alternate deterministically between (1) and (2).
    train_op = Concurrently(
        [store_op, replay_op],
        mode="round_robin",
        # Only return the output
        # of (2) since training metrics are not available until (2) runs.
        output_indexes=[1],
        round_robin_weights=calculate_rr_weights(config))

    return StandardMetricsReporting(train_op, workers, config)


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    if config["framework"] == "torch":
        return CQLTorchPolicy


def after_init(trainer):
    # Add the entire dataset to Replay Buffer (global variable)
    global replay_buffer
    reader = trainer.workers.local_worker().input_reader

    # For d4rl, add the D4RLReaders' dataset to the buffer.
    if "d4rl" in trainer.config["input"]:
        dataset = reader.dataset
        replay_buffer.add_batch(dataset)
    # For a list of files, add each file's entire content to the buffer.
    elif isinstance(reader, ShuffledInput):
        num_batches = 0
        total_timesteps = 0
        for batch in reader.child.read_all_files():
            num_batches += 1
            total_timesteps += len(batch)
            # Add NEXT_OBS if not available. This is slightly hacked
            # as for the very last time step, we will use next-obs=zeros
            # and therefore force-set DONE=True to avoid this missing
            # next-obs to cause learning problems.
            if SampleBatch.NEXT_OBS not in batch:
                obs = batch[SampleBatch.OBS]
                batch[SampleBatch.NEXT_OBS] = \
                    np.concatenate([obs[1:], np.zeros_like(obs[0:1])])
                batch[SampleBatch.DONES][-1] = True
            replay_buffer.add_batch(batch)
        print(
            f"Loaded {num_batches} batches ({total_timesteps} ts) into "
            f"replay buffer, which has capacity {replay_buffer.buffer_size}.")
    else:
        raise ValueError(
            "Unknown offline input! config['input'] must either be list of "
            "offline files (json) or a D4RL-specific InputReader specifier "
            "(e.g. 'd4rl.hopper-medium-v0').")


CQLTrainer = SACTrainer.with_updates(
    name="CQL",
    default_config=CQL_DEFAULT_CONFIG,
    validate_config=validate_config,
    default_policy=CQLTFPolicy,
    get_policy_class=get_policy_class,
    after_init=after_init,
    execution_plan=execution_plan,
)
