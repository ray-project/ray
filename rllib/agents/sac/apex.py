import collections
import copy

import ray
from ray.rllib.agents.dqn.apex import UpdateWorkerWeights
from ray.rllib.agents.dqn.dqn import calculate_rr_weights
from ray.rllib.agents.dqn.learner_thread import LearnerThread
from ray.rllib.agents.sac.sac import DEFAULT_CONFIG as SAC_CONFIG, SACTrainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    STEPS_TRAINED_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.execution.concurrency_ops import Concurrently, Dequeue, Enqueue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import (
    LocalReplayBuffer,
    ReplayActor, ReplayBuffer,
)
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import UpdateTargetNetwork
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.actors import create_colocated
from ray.util.iter import ParallelIteratorWorker
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
)

# yapf: disable
# __sphinx_doc_begin__

APEX_SAC_DEFAULT_CONFIG = SACTrainer.merge_trainer_configs(
    SAC_CONFIG,  # see also the options in sac.py, which are also supported
    {
        "optimizer": {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        },
        "n_step": 1,
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 200000,
        "learning_starts": 5000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 0,
        "timesteps_per_iteration": 1000,
        "exploration_config": {"type": "StochasticSampling"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 10,
        "prioritized_replay": True,
        # If set, this will fix the ratio of sampled to replayed timesteps.
        # Otherwise, replay will proceed as fast as possible.
        "training_intensity": None,
        # Which mode to use in the ParallelRollouts operator used to collect
        # samples. For more details check the operator in rollout_ops module.
        "parallel_rollouts_mode": "async",
        # This only applies if async mode is used (above config setting).
        # Controls the max number of async requests in flight per actor
        "parallel_rollouts_num_async": 2,
    },
)


# __sphinx_doc_end__
# yapf: enable


class LocalAsyncReplayBuffer(LocalReplayBuffer):
    """A replay buffer shard.

    Ray actors are single-threaded, so for scalability multiple replay actors
    may be created to increase parallelism."""

    def __init__(
        self,
        num_shards,
        learning_starts,
        buffer_size,
        replay_batch_size,
        prioritized_replay_alpha=0.6,
        prioritized_replay_beta=0.4,
        prioritized_replay_eps=1e-6,
        multiagent_sync_replay=False,
    ):
        self.replay_starts = learning_starts // num_shards
        self.buffer_size = buffer_size // num_shards
        self.replay_batch_size = replay_batch_size
        self.prioritized_replay_beta = prioritized_replay_beta
        self.prioritized_replay_eps = prioritized_replay_eps
        self.multiagent_sync_replay = multiagent_sync_replay

        def gen_replay():
            while True:
                yield self.replay()

        ParallelIteratorWorker.__init__(self, gen_replay, False)

        def new_buffer():
            return ReplayBuffer(self.buffer_size)

        self.replay_buffers = collections.defaultdict(new_buffer)

        # Metrics
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.update_priorities_timer = TimerStat()
        self.num_added = 0

        # Make externally accessible for testing.
        global _local_replay_buffer
        _local_replay_buffer = self
        # If set, return this instead of the usual data for testing.
        self._fake_batch = None

    def replay(self):
        if self._fake_batch:
            fake_batch = SampleBatch(self._fake_batch)
            return MultiAgentBatch({DEFAULT_POLICY_ID: fake_batch}, fake_batch.count)

        if self.num_added < self.replay_starts:
            return None

        with self.replay_timer:
            samples = {}
            idxes = None
            for policy_id, replay_buffer in self.replay_buffers.items():
                if self.multiagent_sync_replay:
                    if idxes is None:
                        idxes = replay_buffer.sample_idxes(self.replay_batch_size)
                else:
                    idxes = replay_buffer.sample_idxes(self.replay_batch_size)
                (
                    obses_t,
                    actions,
                    rewards,
                    obses_tp1,
                    dones,
                ) = replay_buffer.sample_with_idxes(idxes)
                samples[policy_id] = SampleBatch(
                    {
                        "obs": obses_t,
                        "actions": actions,
                        "rewards": rewards,
                        "new_obs": obses_tp1,
                        "dones": dones,
                        # "weights": weights,
                        # "batch_indexes": batch_indexes,
                    }
                )
            return MultiAgentBatch(samples, self.replay_batch_size)

    def stats(self, debug=False):
        stat = {
            "add_batch_time_ms": round(1000 * self.add_batch_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
        }
        for policy_id, replay_buffer in self.replay_buffers.items():
            stat.update(
                {"policy_{}".format(policy_id): replay_buffer.stats(debug=debug)}
            )
        return stat


AsyncReplayActor = ray.remote(num_cpus=0)(LocalAsyncReplayBuffer)


def async_execution_plan(workers: WorkerSet, config: dict):
    # Create a number of replay buffer actors.
    num_replay_buffer_shards = config["optimizer"]["num_replay_buffer_shards"]
    replay_actor_cls = ReplayActor if config["prioritized_replay"] else AsyncReplayActor
    replay_actors = create_colocated(
        replay_actor_cls,
        [
            num_replay_buffer_shards,
            config["learning_starts"],
            config["buffer_size"],
            config["train_batch_size"],
            config["prioritized_replay_alpha"],
            config["prioritized_replay_beta"],
            config["prioritized_replay_eps"],
        ],
        num_replay_buffer_shards,
    )

    # Start the learner thread.
    learner_thread = LearnerThread(workers.local_worker())
    learner_thread.start()

    # Update experience priorities post learning.
    def update_prio_and_stats(item: ("ActorHandle", dict, int)):
        actor, prio_dict, count = item
        if config["prioritized_replay"]:
            actor.update_priorities.remote(prio_dict)
        metrics = _get_shared_metrics()
        # Manually update the steps trained counter since the learner thread
        # is executing outside the pipeline.
        metrics.counters[STEPS_TRAINED_COUNTER] += count
        metrics.timers["learner_dequeue"] = learner_thread.queue_timer
        metrics.timers["learner_grad"] = learner_thread.grad_timer
        metrics.timers["learner_overall"] = learner_thread.overall_timer

    # We execute the following steps concurrently:
    # (1) Generate rollouts and store them in our replay buffer actors. Update
    # the weights of the worker that generated the batch.
    parallel_rollouts_mode = config.get("parallel_rollouts_mode", "async")
    num_async = config.get("parallel_rollouts_num_async")
    # This could be set to None explicitly
    if not num_async:
        num_async = 2
    rollouts = ParallelRollouts(
        workers, mode=parallel_rollouts_mode, num_async=num_async
    )
    store_op = rollouts.for_each(StoreToReplayBuffer(actors=replay_actors))
    if config.get("execution_plan_custom_store_ops"):
        custom_store_ops = config["execution_plan_custom_store_ops"]
        store_op = store_op.for_each(custom_store_ops(workers, config))
    # Only need to update workers if there are remote workers.
    if workers.remote_workers():
        store_op = store_op.zip_with_source_actor().for_each(
            UpdateWorkerWeights(
                learner_thread,
                workers,
                max_weight_sync_delay=(config["optimizer"]["max_weight_sync_delay"]),
            )
        )

    # (2) Read experiences from the replay buffer actors and send to the
    # learner thread via its in-queue.
    if config.get("before_learn_on_batch"):
        before_learn_on_batch = config["before_learn_on_batch"]
        before_learn_on_batch = before_learn_on_batch(workers, config)
    else:
        before_learn_on_batch = lambda b: b
    replay_op = (
        Replay(actors=replay_actors, num_async=4)
        .for_each(before_learn_on_batch)
        .zip_with_source_actor()
        .for_each(Enqueue(learner_thread.inqueue))
    )

    # (3) Get priorities back from learner thread and apply them to the
    # replay buffer actors.
    update_op = (
        Dequeue(learner_thread.outqueue, check=learner_thread.is_alive)
        .for_each(update_prio_and_stats)
        .for_each(
            UpdateTargetNetwork(
                workers, config["target_network_update_freq"], by_steps_trained=True
            )
        )
    )

    if config["training_intensity"]:
        # Execute (1), (2) with a fixed intensity ratio.
        rr_weights = calculate_rr_weights(config) + ["*"]
        merged_op = Concurrently(
            [store_op, replay_op, update_op],
            mode="round_robin",
            output_indexes=[2],
            round_robin_weights=rr_weights,
        )
    else:
        # Execute (1), (2), (3) asynchronously as fast as possible. Only output
        # items from (3) since metrics aren't available before then.
        merged_op = Concurrently(
            [store_op, replay_op, update_op], mode="async", output_indexes=[2]
        )

    # Add in extra replay and learner metrics to the training result.
    def add_apex_metrics(result):
        replay_stats = ray.get(
            replay_actors[0].stats.remote(config["optimizer"].get("debug"))
        )
        exploration_infos = workers.foreach_trainable_policy(
            lambda p, _: p.get_exploration_info()
        )
        result["info"].update(
            {
                "exploration_infos": exploration_infos,
                "learner_queue": learner_thread.learner_queue_size.stats(),
                "learner": copy.deepcopy(learner_thread.stats),
                "replay_shard_0": replay_stats,
            }
        )
        return result

    # Only report metrics from the workers with the lowest 1/3 of epsilons.
    selected_workers = None
    worker_amount_to_collect_metrics_from = config.get(
        "worker_amount_to_collect_metrics_from"
    )
    if worker_amount_to_collect_metrics_from:
        selected_workers = workers.remote_workers()[
            -len(workers.remote_workers()) // worker_amount_to_collect_metrics_from :
        ]

    return StandardMetricsReporting(
        merged_op, workers, config, selected_workers=selected_workers
    ).for_each(add_apex_metrics)


ApexSACTrainer = SACTrainer.with_updates(
    name="APEX_SAC",
    default_config=APEX_SAC_DEFAULT_CONFIG,
    execution_plan=async_execution_plan,
)
