from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import enum
import logging

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


# Stream partitioning schemes
class PScheme(object):
    def __init__(self, strategy, partition_fn=None):
        self.strategy = strategy
        self.partition_fn = partition_fn

    def __repr__(self):
        return "({},{})".format(self.strategy, self.partition_fn)


# Partitioning strategies
class PStrategy(enum.Enum):
    Forward = 0  # Default
    Shuffle = 1
    Rescale = 2
    RoundRobin = 3
    Broadcast = 4
    Custom = 5
    ShuffleByKey = 6
    # ...


# Operator types
class OpType(enum.Enum):
    Source = 0
    Map = 1
    FlatMap = 2
    Filter = 3
    TimeWindow = 4
    KeyBy = 5
    Sink = 6
    WindowJoin = 7
    Inspect = 8
    ReadTextFile = 9
    Reduce = 10
    Sum = 11
    # ...


# A logical dataflow operator
class Operator(object):
    def __init__(self,
                 id,
                 type,
                 name="",
                 logic=None,
                 num_instances=1,
                 other=None,
                 state_actor=None):
        self.id = id
        self.type = type
        self.name = name
        self.logic = logic  # The operator's logic
        self.num_instances = num_instances
        # One partitioning strategy per downstream operator (default: forward)
        self.partitioning_strategies = {}
        self.other_args = other  # Depends on the type of the operator
        self.state_actor = state_actor  # Actor to query state

    # Sets the partitioning scheme for an output stream of the operator
    def _set_partition_strategy(self,
                                stream_id,
                                partitioning_scheme,
                                dest_operator=None):
        self.partitioning_strategies[stream_id] = (partitioning_scheme,
                                                   dest_operator)

    # Retrieves the partitioning scheme for the given
    # output stream of the operator
    # Returns None is no strategy has been defined for the particular stream
    def _get_partition_strategy(self, stream_id):
        return self.partitioning_strategies.get(stream_id)

    # Cleans metatada from all partitioning strategies that lack a
    # destination operator
    # Valid entries are re-organized as
    # 'destination operator id -> partitioning scheme'
    # Should be called only after the logical dataflow has been constructed
    def _clean(self):
        strategies = {}
        for _, v in self.partitioning_strategies.items():
            strategy, destination_operator = v
            if destination_operator is not None:
                strategies.setdefault(destination_operator, strategy)
        self.partitioning_strategies = strategies

    def print(self):
        log = "Operator<\nID = {}\nName = {}\nType = {}\n"
        log += "Logic = {}\nNumber_of_Instances = {}\n"
        log += "Partitioning_Scheme = {}\nOther_Args = {}>\n"
        logger.debug(
            log.format(self.id, self.name, self.type, self.logic,
                       self.num_instances, self.partitioning_strategies,
                       self.other_args))
