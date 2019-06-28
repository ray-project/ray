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
        # One partitioning strategy per downstream operator (default: rescale)
        self.partitioning_strategies = {}
        # Actor logging
        self.logging = logging
        # An optional mapping of operator instances with local ids in
        # [0..num_instances) to cluster nodes ids
        # Example mapping: ["A","A","B"]: The first two operator instances
        # live in node A whereas the last instance lives in node B
        self.placement = placement
        # TODO (john): Allow partial mappings of instances to nodes
        # Do not allow empty placements for now
        assert(self.placement is not None), (self.placement)
        if self.placement is None:  # Set default mapping (node 0)
            self.placement = ["0"] * num_instances

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

    # Cleans metatada from all partitioning strategies
    # that lack a destination operator
    # Valid entries are re-organized as:
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
        log += "Partitioning_Scheme = {}\nPlacement = {}>\n"
        logger.info(
            log.format(self.id, self.name, self.type, self.logic,
                       self.num_instances, self.partitioning_strategies,
                       self.placement))


class KeyByOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 key_selector,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging)
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
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging,
                          placement)
        self.attribute_selector = attribute_selector

class EventTimeWindowOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 window_length_ms,
                 slide_ms,
                 aggregation_logic=None,
                 offset=0,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging,
                          placement)
        self.window_length_ms = window_length_ms
        self.slide_ms = slide_ms
        self.aggregation_logic = aggregation_logic
        self.offset = offset

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
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging,
                          placement)
        self.other_inputs = other_inputs


class JoinOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 right_input,
                 join_logic,
                 left_input_operator_id,
                 right_input_operator_id,
                 name="",
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          join_logic,
                          num_instances,
                          logging,
                          placement)
        self.right_input = right_input
        # Will be used in actor construction to distinguish left from right
        self.left_input_operator_id = left_input_operator_id
        self.right_input_operator_id = right_input_operator_id


class TimeWindowOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 length,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging)
        self.length = length  # ms


class CustomSourceOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 source_objects,
                 watermark_interval=0,
                 name="",
                 batch_size=None,
                 logic=None,
                 num_instances=1,
                 logging=False,
                 placement=None):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging,
                          placement)
        self.sources = source_objects
        self.watermark_interval = watermark_interval
        self.batch_size = batch_size


class ReadTextFileOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 filepath,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging)
        self.filepath = filepath


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
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging,
                          placement)
        self.sink = sink_object


class WriteTextFileOperator(Operator):
    def __init__(self,
                 id,
                 type,
                 filename_prefix,
                 name="",
                 logic=None,
                 num_instances=1,
                 logging=False):
        Operator.__init__(self,
                          id,
                          type,
                          name,
                          logic,
                          num_instances,
                          logging)
        self.filename_prefix = filename_prefix
