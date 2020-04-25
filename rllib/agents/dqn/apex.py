import collections
import copy

import ray
from ray.rllib.agents.dqn.dqn import DQNTrainer, DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.execution.common import STEPS_TRAINED_COUNTER
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.replay_ops import StoreToReplayBuffer, Replay
from ray.rllib.execution.train_ops import UpdateTargetNetwork
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.optimizers import AsyncReplayOptimizer
from ray.rllib.optimizers.async_replay_optimizer import ReplayActor
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.actors import create_colocated
from ray.rllib.optimizers.async_replay_optimizer import LearnerThread
from ray.util.iter import LocalIterator

# yapf: disable
# __sphinx_doc_begin__
APEX_DEFAULT_CONFIG = merge_dicts(
    DQN_CONFIG,  # see also the options in dqn.py, which are also supported
    {
        "optimizer": merge_dicts(
            DQN_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 3,
        "num_gpus": 1,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "exploration_config": {"type": "PerWorkerEpsilonGreedy"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
    },
)
# __sphinx_doc_end__
# yapf: enable


def defer_make_workers(trainer, env_creator, policy, config):
    # Hack to workaround https://github.com/ray-project/ray/issues/2541
    # The workers will be created later, after the optimizer is created
    return trainer._make_workers(env_creator, policy, config, 0)


def make_async_optimizer(workers, config):
    assert len(workers.remote_workers()) == 0
    extra_config = config["optimizer"].copy()
    for key in [
            "prioritized_replay", "prioritized_replay_alpha",
            "prioritized_replay_beta", "prioritized_replay_eps"
    ]:
        if key in config:
            extra_config[key] = config[key]
    opt = AsyncReplayOptimizer(
        workers,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        train_batch_size=config["train_batch_size"],
        rollout_fragment_length=config["rollout_fragment_length"],
        **extra_config)
    workers.add_workers(config["num_workers"])
    opt._set_workers(workers.remote_workers())
    return opt


def update_target_based_on_num_steps_trained(trainer, fetches):
    # Ape-X updates based on num steps trained, not sampled
    if (trainer.optimizer.num_steps_trained -
            trainer.state["last_target_update_ts"] >
            trainer.config["target_network_update_freq"]):
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = (
            trainer.optimizer.num_steps_trained)
        trainer.state["num_target_updates"] += 1


# Experimental distributed execution impl; enable with "use_exec_api": True.
def execution_plan(workers, config):
    # Create a number of replay buffer actors.
    # TODO(ekl) support batch replay options
    num_replay_buffer_shards = config["optimizer"]["num_replay_buffer_shards"]
    replay_actors = create_colocated(ReplayActor, [
        num_replay_buffer_shards,
        config["learning_starts"],
        config["buffer_size"],
        config["train_batch_size"],
        config["prioritized_replay_alpha"],
        config["prioritized_replay_beta"],
        config["prioritized_replay_eps"],
    ], num_replay_buffer_shards)

    # Update experience priorities post learning.
    def update_prio_and_stats(item):
        actor, prio_dict, count = item
        actor.update_priorities.remote(prio_dict)
        metrics = LocalIterator.get_metrics()
        # Manually update the steps trained counter since the learner thread
        # is executing outside the pipeline.
        metrics.counters[STEPS_TRAINED_COUNTER] += count
        metrics.timers["learner_dequeue"] = learner_thread.queue_timer
        metrics.timers["learner_grad"] = learner_thread.grad_timer
        metrics.timers["learner_overall"] = learner_thread.overall_timer

    # Update worker weights as they finish generating experiences.
    class UpdateWorkerWeights:
        def __init__(self, learner_thread, workers, max_weight_sync_delay):
            self.learner_thread = learner_thread
            self.workers = workers
            self.steps_since_update = collections.defaultdict(int)
            self.max_weight_sync_delay = max_weight_sync_delay
            self.weights = None

        def __call__(self, item):
            actor, batch = item
            self.steps_since_update[actor] += batch.count
            if self.steps_since_update[actor] >= self.max_weight_sync_delay:
                # Note that it's important to pull new weights once
                # updated to avoid excessive correlation between actors.
                if self.weights is None or self.learner_thread.weights_updated:
                    self.learner_thread.weights_updated = False
                    self.weights = ray.put(
                        self.workers.local_worker().get_weights())
                actor.set_weights.remote(self.weights)
                self.steps_since_update[actor] = 0
                # Update metrics.
                metrics = LocalIterator.get_metrics()
                metrics.counters["num_weight_syncs"] += 1

    # Start the learner thread.
    learner_thread = LearnerThread(workers.local_worker())
    learner_thread.start()

    # We execute the following steps concurrently:
    # (1) Generate rollouts and store them in our replay buffer actors. Update
    # the weights of the worker that generated the batch.
    rollouts = ParallelRollouts(workers, mode="async", async_queue_depth=2)
    store_op = rollouts \
        .for_each(StoreToReplayBuffer(actors=replay_actors)) \
        .zip_with_source_actor() \
        .for_each(UpdateWorkerWeights(
            learner_thread, workers,
            max_weight_sync_delay=config["optimizer"]["max_weight_sync_delay"])
        )

    # (2) Read experiences from the replay buffer actors and send to the
    # learner thread via its in-queue.
    replay_op = Replay(actors=replay_actors, async_queue_depth=4) \
        .zip_with_source_actor() \
        .for_each(Enqueue(learner_thread.inqueue))

    # (3) Get priorities back from learner thread and apply them to the
    # replay buffer actors.
    update_op = Dequeue(
            learner_thread.outqueue, check=learner_thread.is_alive) \
        .for_each(update_prio_and_stats) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"],
            by_steps_trained=True))

    # Execute (1), (2), (3) asynchronously as fast as possible. Only output
    # items from (3) since metrics aren't available before then.
    merged_op = Concurrently(
        [store_op, replay_op, update_op], mode="async", output_indexes=[2])

    # Add in extra replay and learner metrics to the training result.
    def add_apex_metrics(result):
        replay_stats = ray.get(replay_actors[0].stats.remote(
            config["optimizer"].get("debug")))
        exploration_infos = workers.foreach_trainable_policy(
            lambda p, _: p.get_exploration_info())
        result["info"].update({
            "exploration_infos": exploration_infos,
            "learner_queue": learner_thread.learner_queue_size.stats(),
            "learner": copy.deepcopy(learner_thread.stats),
            "replay_shard_0": replay_stats,
        })
        return result

    # Only report metrics from the workers with the lowest 1/3 of epsilons.
    selected_workers = workers.remote_workers()[
        -len(workers.remote_workers()) // 3:]

    return StandardMetricsReporting(
        merged_op, workers, config,
        selected_workers=selected_workers).for_each(add_apex_metrics)


APEX_TRAINER_PROPERTIES = {
    "make_workers": defer_make_workers,
    "make_policy_optimizer": make_async_optimizer,
    "after_optimizer_step": update_target_based_on_num_steps_trained,
}

ApexTrainer = DQNTrainer.with_updates(
    name="APEX",
    default_config=APEX_DEFAULT_CONFIG,
    execution_plan=execution_plan,
    **APEX_TRAINER_PROPERTIES)
