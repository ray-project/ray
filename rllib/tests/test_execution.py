import numpy as np
import time
import gym
import queue

import ray
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER
from ray.rllib.execution.concurrency_ops import Concurrently, Enqueue, Dequeue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import StoreToReplayBuffer, Replay
from ray.rllib.execution.rollout_ops import ParallelRollouts, AsyncGradients, \
    ConcatBatches, StandardizeFields
from ray.rllib.execution.train_ops import TrainOneStep, ComputeGradients, \
    AverageGradients
from ray.rllib.execution.buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer, ReplayActor
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.util.iter import LocalIterator, from_range
from ray.util.iter_metrics import SharedMetrics


def iter_list(values):
    return LocalIterator(lambda _: values, SharedMetrics())


def make_workers(n):
    local = RolloutWorker(
        env_creator=lambda _: gym.make("CartPole-v0"),
        policy_spec=PPOTFPolicy,
        rollout_fragment_length=100)
    remotes = [
        RolloutWorker.as_remote().remote(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_spec=PPOTFPolicy,
            rollout_fragment_length=100) for _ in range(n)
    ]
    workers = WorkerSet._from_existing(local, remotes)
    return workers


def test_concurrently(ray_start_regular_shared):
    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="round_robin")
    assert c.take(6) == [1, 4, 2, 5, 3, 6]

    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="async")
    assert c.take(6) == [1, 4, 2, 5, 3, 6]


def test_concurrently_weighted(ray_start_regular_shared):
    a = iter_list([1, 1, 1])
    b = iter_list([2, 2, 2])
    c = iter_list([3, 3, 3])
    c = Concurrently(
        [a, b, c], mode="round_robin", round_robin_weights=[3, 1, 2])
    assert c.take(9) == [1, 1, 1, 2, 3, 3, 2, 3, 2]

    a = iter_list([1, 1, 1])
    b = iter_list([2, 2, 2])
    c = iter_list([3, 3, 3])
    c = Concurrently(
        [a, b, c], mode="round_robin", round_robin_weights=[1, 1, "*"])
    assert c.take(9) == [1, 2, 3, 3, 3, 1, 2, 1, 2]


def test_concurrently_output(ray_start_regular_shared):
    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="round_robin", output_indexes=[1])
    assert c.take(6) == [4, 5, 6]

    a = iter_list([1, 2, 3])
    b = iter_list([4, 5, 6])
    c = Concurrently([a, b], mode="round_robin", output_indexes=[0, 1])
    assert c.take(6) == [1, 4, 2, 5, 3, 6]


def test_enqueue_dequeue(ray_start_regular_shared):
    a = iter_list([1, 2, 3])
    q = queue.Queue(100)
    a.for_each(Enqueue(q)).take(3)
    assert q.qsize() == 3
    assert q.get_nowait() == 1
    assert q.get_nowait() == 2
    assert q.get_nowait() == 3

    q.put("a")
    q.put("b")
    q.put("c")
    a = Dequeue(q)
    assert a.take(3) == ["a", "b", "c"]


def test_metrics(ray_start_regular_shared):
    workers = make_workers(1)
    workers.foreach_worker(lambda w: w.sample())
    a = from_range(10, repeat=True).gather_sync()
    b = StandardMetricsReporting(
        a, workers, {
            "min_iter_time_s": 2.5,
            "timesteps_per_iteration": 0,
            "metrics_num_episodes_for_smoothing": 10,
            "metrics_episode_collection_timeout_s": 10,
        })

    start = time.time()
    res1 = next(b)
    assert res1["episode_reward_mean"] > 0, res1
    res2 = next(b)
    assert res2["episode_reward_mean"] > 0, res2
    assert time.time() - start > 2.4
    workers.stop()


def test_rollouts(ray_start_regular_shared):
    workers = make_workers(2)
    a = ParallelRollouts(workers, mode="bulk_sync")
    assert next(a).count == 200
    counters = a.shared_metrics.get().counters
    assert counters[STEPS_SAMPLED_COUNTER] == 200, counters
    a = ParallelRollouts(workers, mode="async")
    assert next(a).count == 100
    counters = a.shared_metrics.get().counters
    assert counters[STEPS_SAMPLED_COUNTER] == 100, counters
    workers.stop()


def test_rollouts_local(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    assert next(a).count == 100
    counters = a.shared_metrics.get().counters
    assert counters[STEPS_SAMPLED_COUNTER] == 100, counters
    workers.stop()


def test_concat_batches(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="async")
    b = a.combine(ConcatBatches(1000))
    assert next(b).count == 1000
    timers = b.shared_metrics.get().timers
    assert "sample" in timers


def test_standardize(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="async")
    b = a.for_each(StandardizeFields([SampleBatch.EPS_ID]))
    batch = next(b)
    assert abs(np.mean(batch[SampleBatch.EPS_ID])) < 0.001, batch
    assert abs(np.std(batch[SampleBatch.EPS_ID]) - 1.0) < 0.001, batch


def test_async_grads(ray_start_regular_shared):
    workers = make_workers(2)
    a = AsyncGradients(workers)
    res1 = next(a)
    assert isinstance(res1, tuple) and len(res1) == 2, res1
    counters = a.shared_metrics.get().counters
    assert counters[STEPS_SAMPLED_COUNTER] == 100, counters
    workers.stop()


def test_train_one_step(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(TrainOneStep(workers))
    batch, stats = next(b)
    assert isinstance(batch, SampleBatch)
    assert DEFAULT_POLICY_ID in stats
    assert "learner_stats" in stats[DEFAULT_POLICY_ID]
    counters = a.shared_metrics.get().counters
    assert counters[STEPS_SAMPLED_COUNTER] == 100, counters
    assert counters[STEPS_TRAINED_COUNTER] == 100, counters
    timers = a.shared_metrics.get().timers
    assert "learn" in timers
    workers.stop()


def test_compute_gradients(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(ComputeGradients(workers))
    grads, counts = next(b)
    assert counts == 100, counts
    timers = a.shared_metrics.get().timers
    assert "compute_grads" in timers


def test_avg_gradients(ray_start_regular_shared):
    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(ComputeGradients(workers)).batch(4)
    c = b.for_each(AverageGradients())
    grads, counts = next(c)
    assert counts == 400, counts


def test_store_to_replay_local(ray_start_regular_shared):
    buf = MultiAgentReplayBuffer(
        num_shards=1,
        learning_starts=200,
        capacity=1000,
        replay_batch_size=100,
        prioritized_replay_alpha=0.6,
        prioritized_replay_beta=0.4,
        prioritized_replay_eps=0.0001)
    assert buf.replay() is None

    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(StoreToReplayBuffer(local_buffer=buf))

    next(b)
    assert buf.replay() is None  # learning hasn't started yet
    next(b)
    assert buf.replay().count == 100

    replay_op = Replay(local_buffer=buf)
    assert next(replay_op).count == 100


def test_store_to_replay_actor(ray_start_regular_shared):
    actor = ReplayActor.remote(
        num_shards=1,
        learning_starts=200,
        buffer_size=1000,
        replay_batch_size=100,
        prioritized_replay_alpha=0.6,
        prioritized_replay_beta=0.4,
        prioritized_replay_eps=0.0001)
    assert ray.get(actor.replay.remote()) is None

    workers = make_workers(0)
    a = ParallelRollouts(workers, mode="bulk_sync")
    b = a.for_each(StoreToReplayBuffer(actors=[actor]))

    next(b)
    assert ray.get(actor.replay.remote()) is None  # learning hasn't started
    next(b)
    assert ray.get(actor.replay.remote()).count == 100

    replay_op = Replay(actors=[actor])
    assert next(replay_op).count == 100


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
