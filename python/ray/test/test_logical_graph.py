from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.streaming import Environment
from ray.experimental.streaming.operator import OpType, PStrategy

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def test_parallelism():
    """Tests operator parallelism."""
    env = Environment()
    # Try setting a common parallelism for all operators
    env.set_parallelism(2)
    stream = env.source(None).map(None).filter(None).flat_map(None)
    env._collect_garbage()
    for operator in env.operators.values():
        if operator.type == OpType.Source:
            # TODO (john): Currently each source has only one instance
            assert operator.num_instances == 1, (operator.num_instances, 1)
        else:
            assert operator.num_instances == 2, (operator.num_instances, 2)
    # Check again after adding an operator with different parallelism
    stream.map(None, "Map1").shuffle().set_parallelism(3).map(
        None, "Map2").set_parallelism(4)
    env._collect_garbage()
    for operator in env.operators.values():
        if operator.type == OpType.Source:
            assert operator.num_instances == 1, (operator.num_instances, 1)
        elif operator.name != "Map1" and operator.name != "Map2":
            assert operator.num_instances == 2, (operator.num_instances, 2)
        elif operator.name != "Map2":
            assert operator.num_instances == 3, (operator.num_instances, 3)
        else:
            assert operator.num_instances == 4, (operator.num_instances, 4)


def test_partitioning():
    """Tests stream partitioning."""
    env = Environment()
    # Try defining multiple partitioning strategies for the same stream
    stream = env.source(None).shuffle().rescale().broadcast().map(
        None).broadcast().shuffle()
    env._collect_garbage()
    for operator in env.operators.values():
        p_schemes = operator.partitioning_strategies
        for scheme in p_schemes.values():
            # Only last defined strategy should be kept
            if operator.type == OpType.Source:
                assert scheme.strategy == PStrategy.Broadcast, (
                    scheme.strategy, PStrategy.Broadcast)
            else:
                assert scheme.strategy == PStrategy.Shuffle, (
                    scheme.strategy, PStrategy.Shuffle)


def test_forking():
    """Tests stream forking."""
    env = Environment()
    # Try forking a stream
    stream = env.source(None).map(None).set_parallelism(2)
    # First branch
    stream_2 = stream.shuffle().key_by(0).sum(1)
    # Second branch
    stream_3 = stream.key_by(1).sum(2)
    env._collect_garbage()
    # Operator ids
    source_id = None
    map_id = None
    keyby1_id = None
    keyby2_id = None
    sum1_id = None
    sum2_id = None
    # Collect ids
    for id, operator in env.operators.items():
        if operator.type == OpType.Source:
            source_id = id
        elif operator.type == OpType.Map:
            map_id = id
        elif operator.type == OpType.KeyBy:
            if operator.other_args == 0:
                keyby1_id = id
            else:
                assert operator.other_args == 1, (operator.other_args, 1)
                keyby2_id = id
        elif operator.type == OpType.Sum:
            if operator.other_args == 1:
                sum1_id = id
            else:
                assert operator.other_args == 2, (operator.other_args, 2)
                sum2_id = id
    # Check dataflow edges and stream partitioning strategies
    for source, destination in env.logical_topo.edges:
        if source == source_id:
            assert destination == map_id, (destination, map_id)
        elif source == map_id:
            operator = env.operators[map_id]
            key_index = env.operators[destination].other_args
            p_scheme = operator.partitioning_strategies[destination]
            strategy = p_scheme.strategy
            if key_index == 0:  # This must be the first branch
                assert strategy == PStrategy.Shuffle, (strategy,
                                                       PStrategy.Shuffle)
                assert destination == keyby1_id, (destination, keyby1_id)
            else:  # This must be the second branch
                assert key_index == 1, (key_index, 1)
                assert strategy == PStrategy.Forward, (strategy,
                                                       PStrategy.Forward)
                assert destination == keyby2_id, (destination, keyby2_id)
        elif source == keyby1_id or source == keyby2_id:
            operator = env.operators[source]
            p_scheme = operator.partitioning_strategies[destination]
            key_index = env.operators[destination].other_args
            if key_index == 1:  # This must be the first branch
                assert strategy == PStrategy.Forward, (strategy,
                                                       PStrategy.Forward)
                assert destination == sum1_id, (destination, sum1_id)
            else:  # This must be the second branch
                assert key_index == 2, (key_index, 2)
                assert strategy == PStrategy.Forward, (strategy,
                                                       PStrategy.Forward)
                assert destination == sum2_id, (destination, sum2_id)
        else:  # This must be a sum operator
            operator = env.operators[source]
            assert operator.type == OpType.Sum, (operator.type, OpType.Sum)


def test_channel_generation():
    """Tests data channel generation."""
    env = Environment()
    # Try creating three stages with different connectivity
    stream = env.source(None).shuffle().map(None).set_parallelism(4).filter(
        None).set_parallelism(2).broadcast().flat_map(None).set_parallelism(3)
    env._collect_garbage()
    # Operator ids
    source_id = None
    map_id = None
    filter_id = None
    flatmap_id = None
    # Collect ids
    for id, operator in env.operators.items():
        if operator.type == OpType.Source:
            source_id = id
        elif operator.type == OpType.Map:
            map_id = id
        elif operator.type == OpType.Filter:
            filter_id = id
        elif operator.type == OpType.FlatMap:
            flatmap_id = id
    # Collect channels
    channels_per_stage = []
    for operator in env.operators.values():
        channels_per_stage.append(env._generate_channels(operator))
    # Check actual connections
    expected_1 = [(0, 0), (0, 1), (0, 2), (0, 3)]
    expected_2 = [(0, 0), (1, 1), (2, 0), (3, 1)]
    expected_3 = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
    stage_1 = []
    stage_2 = []
    stage_3 = []
    for stage in channels_per_stage:
        for channels in stage.values():
            for channel in channels:
                src_instance_id = channel.src_instance_id
                dst_instance_id = channel.dst_instance_id
                connection = (src_instance_id, dst_instance_id)
                if channel.src_operator_id == source_id:
                    assert channel.dst_operator_id == map_id, (
                        channel.dst_operator_id, map_id)
                    stage_1.append(connection)
                if channel.src_operator_id == map_id:
                    assert channel.dst_operator_id == filter_id, (
                        channel.dst_operator_id, filter_id)
                    stage_2.append(connection)
                if channel.src_operator_id == filter_id:
                    assert channel.dst_operator_id == flatmap_id, (
                        channel.dst_operator_id, flatmap_id)
                    stage_3.append(connection)
    # Make sure connections are as expected
    for connection in expected_1:
        assert connection in stage_1, (connection, stage_1)
    for connection in stage_1:
        assert connection in expected_1, (connection, expected_1)
    for connection in expected_2:
        assert connection in stage_2, (connection, stage_2)
    for connection in stage_2:
        assert connection in expected_2, (connection, expected_2)
    for connection in expected_3:
        assert connection in stage_3, (connection, stage_3)
    for connection in stage_3:
        assert connection in expected_3, (connection, expected_3)


# TODO (john): Add simple wordcount test
def test_wordcount():
    """Tests a simple streaming wordcount."""
    pass


if __name__ == "__main__":
    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    logger.info("== Testing Logical Graph Construction ==")
    # Test operator parallelism
    test_parallelism()
    # Test stream partitioning strategies
    test_partitioning()
    # Test stream forking
    test_forking()
    # Test data channel generation
    test_channel_generation()
    # Test streaming wordcount
    test_wordcount()
