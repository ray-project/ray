from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.experimental.streaming.streaming import Environment
from ray.experimental.streaming.operator import OpType, PStrategy


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
    _ = env.source(None).shuffle().rescale().broadcast().map(
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
    # First branch with a shuffle partitioning strategy
    _ = stream.shuffle().key_by(0).sum(1)
    # Second branch with the default partitioning strategy
    _ = stream.key_by(1).sum(2)
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
    # Check generated streams and their partitioning
    for source, destination in env.logical_topo.edges:
        operator = env.operators[source]
        if source == source_id:
            assert destination == map_id, (destination, map_id)
        elif source == map_id:
            p_scheme = operator.partitioning_strategies[destination]
            strategy = p_scheme.strategy
            key_index = env.operators[destination].other_args
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
            p_scheme = operator.partitioning_strategies[destination]
            strategy = p_scheme.strategy
            key_index = env.operators[destination].other_args
            if key_index == 1:  # This must be the first branch
                assert strategy == PStrategy.ShuffleByKey, (
                    strategy, PStrategy.ShuffleByKey)
                assert destination == sum1_id, (destination, sum1_id)
            else:  # This must be the second branch
                assert key_index == 2, (key_index, 2)
                assert strategy == PStrategy.ShuffleByKey, (
                    strategy, PStrategy.ShuffleByKey)
                assert destination == sum2_id, (destination, sum2_id)
        else:  # This must be a sum operator
            assert operator.type == OpType.Sum, (operator.type, OpType.Sum)


def _test_shuffle_channels():
    """Tests shuffling connectivity."""
    env = Environment()
    # Try defining a shuffle
    _ = env.source(None).shuffle().map(None).set_parallelism(4)
    expected = [(0, 0), (0, 1), (0, 2), (0, 3)]
    _test_channels(env, expected)


def _test_forward_channels():
    """Tests forward connectivity."""
    env = Environment()
    # Try the default partitioning strategy
    _ = env.source(None).set_parallelism(4).map(None).set_parallelism(2)
    expected = [(0, 0), (1, 1), (2, 0), (3, 1)]
    _test_channels(env, expected)


def _test_broadcast_channels():
    """Tests broadcast connectivity."""
    env = Environment()
    # Try broadcasting
    _ = env.source(None).set_parallelism(4).broadcast().map(
        None).set_parallelism(2)
    expected = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1), (3, 0), (3, 1)]
    _test_channels(env, expected)


def _test_round_robin_channels():
    """Tests round-robin connectivity."""
    env = Environment()
    # Try broadcasting
    _ = env.source(None).round_robin().map(None).set_parallelism(2)
    expected = [(0, 0), (0, 1)]
    _test_channels(env, expected)


def _test_channels(environment, expected_channels):
    """Tests operator connectivity."""
    environment._collect_garbage()
    map_id = None
    # Get id
    for id, operator in environment.operators.items():
        if operator.type == OpType.Map:
            map_id = id
    # Collect channels
    channels_per_destination = []
    for operator in environment.operators.values():
        channels_per_destination.append(
            environment._generate_channels(operator))
    # Check actual connectivity
    actual = []
    for destination in channels_per_destination:
        for channels in destination.values():
            for channel in channels:
                src_instance_id = channel.src_instance_id
                dst_instance_id = channel.dst_instance_id
                connection = (src_instance_id, dst_instance_id)
                assert channel.dst_operator_id == map_id, (
                    channel.dst_operator_id, map_id)
                actual.append(connection)
    # Make sure connections are as expected
    set_1 = set(expected_channels)
    set_2 = set(actual)
    assert set_1 == set_2, (set_1, set_2)


def test_channel_generation():
    """Tests data channel generation."""
    _test_shuffle_channels()
    _test_broadcast_channels()
    _test_round_robin_channels()
    _test_forward_channels()


# TODO (john): Add simple wordcount test
def test_wordcount():
    """Tests a simple streaming wordcount."""
    pass
