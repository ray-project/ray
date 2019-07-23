from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import enum
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# Stream partitioning schemes
class PScheme(object):
    def __init__(self, strategy, partition_fn=None):
        self.strategy = strategy
        self.partition_fn = partition_fn

    # For pretty print
    def __repr__(self):
        return "({},{})".format(self.strategy, self.partition_fn)


# Partitioning strategies
class PStrategy(enum.Enum):
    Rescale = 0  # Default
    Shuffle = 1
    Forward = 2
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
    Union = 12
    WriteTextFile = 13
    Join = 14
    EventTimeWindow = 15
    # ...


# A logical dataflow operator
class Operator(object):
    def __init__(self,
                 id,
                 type,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        self.id = id
        self.type = type
        self.name = name
        self.logic = logic  # The operator's logic
        self.num_instances = num_instances
        # One PStrategy per downstream operator (default: Rescale)
        self.partitioning_strategies = {}
        # Actor logging flag
        self.logging = logging
        # A mapping of operator instances with local ids in
        # [0..num_instances) to cluster node ids
        # Example mapping: ["A","A","B"]: The first two operator instances
        # live in node A whereas the last instance lives in node with id B
        self.placement = placement
        # TODO (john): Assign operator instances to nodes automatically if
        # no placement is specified. Do not allow empty placements for now
        assert self.placement is not None

    # Sets the partitioning scheme for an output stream of the operator
    def _set_partition_strategy(self,
                                stream_id,
                                partitioning_scheme,
                                dest_operator=None):
        self.partitioning_strategies[stream_id] = (partitioning_scheme,
                                                   dest_operator)

    # Retrieves the partitioning scheme for the given output stream
    # Returns None is no strategy has been defined for the particular stream
    def _get_partition_strategy(self, stream_id):
        return self.partitioning_strategies.get(stream_id)

    # Cleans all partitioning strategies that lack a destination operator and
    # reorganizes entries as: 'destination operator id -> partitioning scheme'
    def _clean(self):
        strategies = {}
        for _, v in self.partitioning_strategies.items():
            strategy, destination_operator = v
            if destination_operator is not None:
                strategies.setdefault(destination_operator, strategy)
        self.partitioning_strategies = strategies

    # For pretty print
    def print(self):
        op = "Operator<\nID = {}\nName = {}\nType = {}\n"
        op += "Logic = {}\nNumber_of_Instances = {}\n"
        op += "Partitioning_Scheme = {}\nPlacement = {}>\n"
        logger.info(
            op.format(self.id, self.name, self.type, self.logic,
                      self.num_instances, self.partitioning_strategies,
                      self.placement))


class CustomSourceOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 source_object,
                 watermark_interval=0,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, logic, num_instances, logging,
                          placement)
        # One source object for each instance of the source operator
        self.source = source_object
        self.watermark_interval = watermark_interval  # in ms (default: 0)


class CustomSinkOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 sink_object,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, logic, num_instances, logging,
                          placement)
        self.sink = sink_object


class KeyByOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 key_selector,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, logic, num_instances, logging,
                          placement)
        self.key_selector = key_selector


class SumOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 attribute_selector,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, logic, num_instances, logging,
                          placement)
        self.attribute_selector = attribute_selector


class UnionOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 other_inputs,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, logic, num_instances, logging,
                          placement)
        self.other_inputs = other_inputs


class JoinOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 join_logic,
                 left_input_operator_id,
                 right_input_operator_id,
                 name="",
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self, id, type, name, join_logic, num_instances,
                          logging, placement)
        # Used to distinguish left from right input
        self.left_input_operator_id = left_input_operator_id
        self.right_input_operator_id = right_input_operator_id
