import functools
import math
import time
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import ray
from ray.data._internal.execution.backpressure_policy import (
    ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
    ConcurrencyCapBackpressurePolicy,
    StreamingOutputBackpressurePolicy,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
)
from ray.tests.conftest import shutdown_only  # noqa: F401


class TestConcurrencyCapBackpressurePolicy(unittest.TestCase):
    """Tests for ConcurrencyCapBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 10
        ray.init(num_cpus=cls._cluster_cpus)
        data_context = ray.data.DataContext.get_current()
        data_context.set_config(
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
            [ConcurrencyCapBackpressurePolicy],
        )

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
        data_context = ray.data.DataContext.get_current()
        data_context.remove_config(ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY)

    def test_basic(self):
        concurrency = 16
        input_op = InputDataBuffer(input_data=[MagicMock()])
        map_op_no_concurrency = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            input_op=input_op,
            target_max_block_size=None,
        )
        map_op = TaskPoolMapOperator(
            map_transformer=MagicMock(),
            input_op=map_op_no_concurrency,
            target_max_block_size=None,
            concurrency=concurrency,
        )
        map_op.metrics.num_tasks_running = 0
        map_op.metrics.num_tasks_finished = 0
        topology = {
            map_op: MagicMock(),
            input_op: MagicMock(),
            map_op_no_concurrency: MagicMock(),
        }

        policy = ConcurrencyCapBackpressurePolicy(topology)

        self.assertEqual(policy._concurrency_caps[map_op], concurrency)
        self.assertTrue(math.isinf(policy._concurrency_caps[input_op]))
        self.assertTrue(math.isinf(policy._concurrency_caps[map_op_no_concurrency]))

        # Gradually increase num_tasks_running to the cap.
        for i in range(1, concurrency + 1):
            self.assertTrue(policy.can_add_input(map_op))
            map_op.metrics.num_tasks_running = i
        # Now num_tasks_running reaches the cap, so can_add_input should return False.
        self.assertFalse(policy.can_add_input(map_op))

        map_op.metrics.num_tasks_running = concurrency / 2
        self.assertEqual(policy.can_add_input(map_op), True)

    def _create_record_time_actor(self):
        @ray.remote(num_cpus=0)
        class RecordTimeActor:
            def __init__(self):
                self._start_time = defaultdict(lambda: [])
                self._end_time = defaultdict(lambda: [])

            def record_start_time(self, index):
                self._start_time[index].append(time.time())

            def record_end_time(self, index):
                self._end_time[index].append(time.time())

            def get_start_and_end_time_for_op(self, index):
                return min(self._start_time[index]), max(self._end_time[index])

            def get_start_and_end_time_for_all_tasks_of_op(self, index):
                return self._start_time[index], self._end_time[index]

        actor = RecordTimeActor.remote()
        return actor

    def _get_map_func(self, actor, index):
        def map_func(data, actor, index):
            actor.record_start_time.remote(index)
            yield data
            actor.record_end_time.remote(index)

        return functools.partial(map_func, actor=actor, index=index)

    def test_e2e_normal(self):
        """A simple E2E test with ConcurrencyCapBackpressurePolicy enabled."""
        actor = self._create_record_time_actor()
        map_func1 = self._get_map_func(actor, 1)
        map_func2 = self._get_map_func(actor, 2)

        # Creat a dataset with 2 map ops. Each map op has N tasks, where N is
        # the number of cluster CPUs.
        N = self.__class__._cluster_cpus
        ds = ray.data.range(N, parallelism=N)
        # Use different `num_cpus` to make sure they don't fuse.
        ds = ds.map_batches(map_func1, batch_size=None, num_cpus=1, concurrency=1)
        ds = ds.map_batches(map_func2, batch_size=None, num_cpus=1.1, concurrency=1)
        res = ds.take_all()
        self.assertEqual(len(res), N)

        # We recorded the start and end time of each op,
        # check that these 2 ops are executed interleavingly.
        # This means that the executor didn't allocate all resources to the first
        # op in the beginning.
        start1, end1 = ray.get(actor.get_start_and_end_time_for_op.remote(1))
        start2, end2 = ray.get(actor.get_start_and_end_time_for_op.remote(2))
        assert start1 < start2 < end1 < end2, (start1, start2, end1, end2)


class TestStreamOutputBackpressurePolicy(unittest.TestCase):
    """Tests for StreamOutputBackpressurePolicy."""

    @classmethod
    def setUpClass(cls):
        cls._cluster_cpus = 5
        cls._cluster_object_memory = 500 * 1024 * 1024
        ray.init(
            num_cpus=cls._cluster_cpus, object_store_memory=cls._cluster_object_memory
        )
        data_context = ray.data.DataContext.get_current()
        cls._num_blocks = 5
        cls._block_size = 100 * 1024 * 1024
        policy_cls = StreamingOutputBackpressurePolicy
        cls._max_blocks_in_op_output_queue = 1
        cls._max_blocks_in_generator_buffer = 1
        cls._configs = {
            ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY: [policy_cls],
            policy_cls.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY: (
                cls._max_blocks_in_op_output_queue
            ),
            policy_cls.MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY: (
                cls._max_blocks_in_generator_buffer
            ),
        }
        for k, v in cls._configs.items():
            data_context.set_config(k, v)
        data_context.execution_options.preserve_order = True

    @classmethod
    def tearDownClass(cls):
        data_context = ray.data.DataContext.get_current()
        for k in cls._configs.keys():
            data_context.remove_config(k)
        data_context.execution_options.preserve_order = False
        ray.shutdown()

    def _create_mock_op_and_op_state(
        self,
        name,
        outqueue_num_blocks=0,
        num_active_tasks=0,
        num_outputs_generated=0,
    ):
        op = MagicMock()
        op.__str__.return_value = f"Op({name})"
        op.num_active_tasks.return_value = num_active_tasks
        op.metrics.num_outputs_generated = num_outputs_generated

        state = MagicMock()
        state.__str__.return_value = f"OpState({name})"
        state.outqueue_num_blocks.return_value = outqueue_num_blocks

        state.op = op
        return op, state

    def test_policy_basic(self):
        """Basic unit test for the policy without real execution."""
        up_op, up_state = self._create_mock_op_and_op_state("up")
        down_op, down_state = self._create_mock_op_and_op_state("down")
        topology = {}
        topology[up_op] = up_state
        topology[down_op] = down_state

        policy = StreamingOutputBackpressurePolicy(topology)
        assert (
            policy._max_num_blocks_in_op_output_queue
            == self._max_blocks_in_op_output_queue
        )
        data_context = ray.data.DataContext.get_current()
        assert (
            data_context._max_num_blocks_in_streaming_gen_buffer
            == self._max_blocks_in_generator_buffer
        )

        # Buffers are empty, both ops can read up to the max.
        res = policy.calculate_max_blocks_to_read_per_op(topology)
        assert res == {
            up_state: self._max_blocks_in_op_output_queue,
            down_state: self._max_blocks_in_op_output_queue,
        }

        # up_op's buffer is full, but down_up has no active tasks.
        # We'll still allow up_op to read 1 block.
        up_state.outqueue_num_blocks.return_value = self._max_blocks_in_op_output_queue
        res = policy.calculate_max_blocks_to_read_per_op(topology)
        assert res == {
            up_state: 1,
            down_state: self._max_blocks_in_op_output_queue,
        }

        # down_op now has 1 active task. So we won't allow up_op to read any more.
        down_op.num_active_tasks.return_value = 1
        res = policy.calculate_max_blocks_to_read_per_op(topology)
        assert res == {
            up_state: 0,
            down_state: self._max_blocks_in_op_output_queue,
        }

        # After `MAX_OUTPUT_IDLE_SECONDS` of no outputs from down_up,
        # we'll allow up_op to read 1 block again.
        with patch.object(
            StreamingOutputBackpressurePolicy, "MAX_OUTPUT_IDLE_SECONDS", 0.1
        ):
            time.sleep(0.11)
            res = policy.calculate_max_blocks_to_read_per_op(topology)
            assert res == {
                up_state: 1,
                down_state: self._max_blocks_in_op_output_queue,
            }

            # down_up now has outputs, so we won't allow up_op to read any more.
            down_op.metrics.num_outputs_generated = 1
            res = policy.calculate_max_blocks_to_read_per_op(topology)
            assert res == {
                up_state: 0,
                down_state: self._max_blocks_in_op_output_queue,
            }

    def _run_dataset(self, producer_num_cpus, consumer_num_cpus):
        # Create a dataset with 2 operators:
        # - The producer op has only 1 task, which produces 5 blocks, each of which
        #   has 100MB data.
        # - The consumer op has 5 slow tasks, each of which consumes 1 block.
        # Return the timestamps at the producer and consumer tasks for each block.
        num_blocks = self._num_blocks
        block_size = self._block_size
        ray.data.DataContext.get_current().target_max_block_size = block_size

        def producer(batch):
            for i in range(num_blocks):
                print("Producing block", i)
                yield {
                    "id": [i],
                    "data": [np.zeros(block_size, dtype=np.uint8)],
                    "producer_timestamp": [time.time()],
                }

        def consumer(batch):
            assert len(batch["id"]) == 1
            print("Consuming block", batch["id"][0])
            batch["consumer_timestamp"] = [time.time()]
            time.sleep(0.1)
            del batch["data"]
            return batch

        ds = ray.data.range(1, parallelism=1).materialize()
        ds = ds.map_batches(producer, batch_size=None, num_cpus=producer_num_cpus)
        ds = ds.map_batches(consumer, batch_size=None, num_cpus=consumer_num_cpus)

        res = ds.take_all()
        assert [row["id"] for row in res] == list(range(self._num_blocks))
        return (
            [row["producer_timestamp"] for row in res],
            [row["consumer_timestamp"] for row in res],
        )

    def test_no_deadlock(self):
        # The producer needs all 5 CPUs, and the consumer has no CPU to run.
        # In this case, we shouldn't backpressure the producer and let it run
        # until it finishes.
        producer_timestamps, consumer_timestamps = self._run_dataset(
            producer_num_cpus=5, consumer_num_cpus=1
        )
        assert producer_timestamps[-1] < consumer_timestamps[0], (
            producer_timestamps,
            consumer_timestamps,
        )

    def test_no_deadlock_for_resource_contention(self):
        """Test no deadlock in case of resource contention from
        non-Data code."""
        # Create a non-Data actor that uses 4 CPUs, only 1 CPU
        # is left for Data. Currently Data StreamExecutor still
        # incorrectly assumes it has all the 5 CPUs.
        # Check that we don't deadlock in this case.

        @ray.remote(num_cpus=4)
        class DummyActor:
            def foo(self):
                return None

        dummy_actor = DummyActor.remote()
        ray.get(dummy_actor.foo.remote())

        producer_timestamps, consumer_timestamps = self._run_dataset(
            producer_num_cpus=1, consumer_num_cpus=0.9
        )
        assert producer_timestamps[-1] < consumer_timestamps[0], (
            producer_timestamps,
            consumer_timestamps,
        )


def test_large_e2e_backpressure(shutdown_only, restore_data_context):  # noqa: F811
    """Test backpressure on a synthetic large-scale workload."""
    # The cluster has 10 CPUs and 200MB object store memory.
    # The dataset will have 200MB * 25% = 50MB memory budget.
    #
    # Each produce task generates 10 blocks, each of which has 10MB data.
    # In total, there will be 10 * 10 * 10MB = 1000MB intermediate data.
    #
    # With StreamingOutputBackpressurePolicy and the following configuration,
    # the executor will still schedule 10 produce tasks, but only the first task is
    # allowed to output all blocks. The total size of pending blocks will be
    # (10 + 9 * 1 + 1) * 10MB = 200MB, where
    # - 10 is the number of blocks in the first task.
    # - 9 * 1 is the number of blocks pending at the streaming generator level of
    #   the other 9 tasks.
    # - 1 is the number of blocks pending at the output queue.

    NUM_CPUS = 10
    NUM_ROWS_PER_TASK = 10
    NUM_TASKS = 20
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS
    BLOCK_SIZE = 10 * 1024 * 1024
    STREMING_GEN_BUFFER_SIZE = 1
    OP_OUTPUT_QUEUE_SIZE = 1
    max_pending_block_bytes = (
        NUM_ROWS_PER_TASK
        + (NUM_CPUS - 1) * STREMING_GEN_BUFFER_SIZE
        + OP_OUTPUT_QUEUE_SIZE
    ) * BLOCK_SIZE
    # Set the object store memory to be slightly larger than the pending data
    # size because object store spilling is triggered at 80% capacity.
    object_store_memory = max_pending_block_bytes / 0.8 + BLOCK_SIZE
    print(f"max_pending_block_bytes: {max_pending_block_bytes/1024/1024}MB")
    print(f"object_store_memory: {object_store_memory/1024/1024}MB")

    ray.init(num_cpus=NUM_CPUS, object_store_memory=object_store_memory)

    def produce(batch):
        print("Produce task started", batch["id"])
        time.sleep(0.1)
        for id in batch["id"]:
            print("Producing", id)
            yield {
                "id": [id],
                "image": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
            }

    def consume(batch):
        print("Consume task started", batch["id"])
        time.sleep(0.01)
        return {"id": batch["id"], "result": [0 for _ in batch["id"]]}

    data_context = ray.data.DataContext.get_current()
    data_context.set_config(
        ENABLED_BACKPRESSURE_POLICIES_CONFIG_KEY,
        [
            StreamingOutputBackpressurePolicy,
        ],
    )
    data_context.set_config(
        StreamingOutputBackpressurePolicy.MAX_BLOCKS_IN_OP_OUTPUT_QUEUE_CONFIG_KEY,
        OP_OUTPUT_QUEUE_SIZE,
    )
    data_context.set_config(
        StreamingOutputBackpressurePolicy.MAX_BLOCKS_IN_GENERATOR_BUFFER_CONFIG_KEY,
        STREMING_GEN_BUFFER_SIZE,
    )
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = ray.data.range(NUM_ROWS_TOTAL, parallelism=NUM_TASKS)
    ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
    ds = ds.map_batches(consume, batch_size=None, num_cpus=0.9)
    # Check core execution metrics every 10 rows, because it's expensive.
    for _ in ds.iter_batches(batch_size=NUM_ROWS_PER_TASK):
        # The amount of generated data should be less than
        # max_pending_block_bytes (pending data) +
        # NUM_ROWS_PER_TASK * BLOCK_SIZE (consumed data)
        max_created_bytes_per_consumption = (
            max_pending_block_bytes + NUM_ROWS_PER_TASK * BLOCK_SIZE
        )

        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                object_store_stats={
                    "spilled_bytes_total": 0,
                    "restored_bytes_total": 0,
                    "cumulative_created_plasma_bytes": lambda x: x
                    <= 1.5 * max_created_bytes_per_consumption,
                },
            ),
            last_snapshot,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
