from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys
import uuid

import networkx as nx

from ray.experimental.streaming.communication import DataChannel, DataInput
from ray.experimental.streaming.communication import DataOutput, QueueConfig
from ray.experimental.streaming.operator import Operator, OpType
from ray.experimental.streaming.operator import PScheme, PStrategy
import ray.experimental.streaming.operator_instance as operator_instance

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Generates UUIDs
def _generate_uuid():
    return uuid.uuid4()


# Rolling sum's logic
def _sum(value_1, value_2):
    return value_1 + value_2


# Partitioning strategies that require all-to-all instance communication
all_to_all_strategies = [
    PStrategy.Shuffle, PStrategy.ShuffleByKey, PStrategy.Broadcast,
    PStrategy.RoundRobin
]


# Environment configuration
class Config(object):
    """Environment configuration.

    This class includes all information about the configuration of the
    streaming environment.

    Attributes:
         queue_config (QueueConfig): Batched Queue configuration
         (see: communication.py)
         A batched queue configuration includes the max queue size,
         the size of each batch (in number of elements), the batch flush
         timeout, and the number of batches to prefetch from plasma
         parallelism (int): The number of isntances (actors) for each logical
         dataflow operator (default: 1)
    """

    def __init__(self, parallelism=1):
        self.queue_config = QueueConfig()
        self.parallelism = parallelism
        # ...


# The execution environment for a streaming job
class Environment(object):
    """A streaming environment.

    This class is responsible for constructing the logical and the
    physical dataflow.

    Attributes:
         logical_topo (DiGraph): The user-defined logical topology in
         NetworkX DiGRaph format.
         (See: https://networkx.github.io)
         physical_topo (DiGraph): The physical topology in NetworkX
         DiGRaph format. The physical dataflow is constructed by the
         environment based on logical_topo.
         operators (dict): A mapping from operator ids to operator metadata
         (See: Operator in operator.py).
         config (Config): The environment's configuration.
         topo_cleaned (bool): A flag that indicates whether the logical
         topology is garbage collected (True) or not (False).
         actor_handles (list): A list of all Ray actor handles that execute
         the streaming dataflow.
    """

    def __init__(self, config=Config()):
        self.logical_topo = nx.DiGraph()  # DAG
        self.physical_topo = nx.DiGraph()  # DAG
        self.operators = {}  # operator id --> operator object
        self.config = config  # Environment's configuration
        self.topo_cleaned = False
        # Handles to all actors in the physical dataflow
        self.actor_handles = []

    # Constructs and deploys a Ray actor of a specific type
    # TODO (john): Actor placement information should be specified in
    # the environment's configuration
    def __generate_actor(self, instance_id, operator, input, output):
        """Generates an actor that will execute a particular instance of
        the logical operator

        Attributes:
            instance_id (UUID): The id of the instance the actor will execute.
            operator (Operator): The metadata of the logical operator.
            input (DataInput): The input gate that manages input channels of
            the instance (see: DataInput in communication.py).
            input (DataOutput): The output gate that manages output channels
            of the instance (see: DataOutput in communication.py).
        """
        actor_id = (operator.id, instance_id)
        # Record the physical dataflow graph (for debugging purposes)
        self.__add_channel(actor_id, input, output)
        # Select actor to construct
        if operator.type == OpType.Source:
            source = operator_instance.Source.remote(actor_id, operator, input,
                                                     output)
            source.register_handle.remote(source)
            return source.start.remote()
        elif operator.type == OpType.Map:
            map = operator_instance.Map.remote(actor_id, operator, input,
                                               output)
            map.register_handle.remote(map)
            return map.start.remote()
        elif operator.type == OpType.FlatMap:
            flatmap = operator_instance.FlatMap.remote(actor_id, operator,
                                                       input, output)
            flatmap.register_handle.remote(flatmap)
            return flatmap.start.remote()
        elif operator.type == OpType.Filter:
            filter = operator_instance.Filter.remote(actor_id, operator, input,
                                                     output)
            filter.register_handle.remote(filter)
            return filter.start.remote()
        elif operator.type == OpType.Reduce:
            reduce = operator_instance.Reduce.remote(actor_id, operator, input,
                                                     output)
            reduce.register_handle.remote(reduce)
            return reduce.start.remote()
        elif operator.type == OpType.TimeWindow:
            pass
        elif operator.type == OpType.KeyBy:
            keyby = operator_instance.KeyBy.remote(actor_id, operator, input,
                                                   output)
            keyby.register_handle.remote(keyby)
            return keyby.start.remote()
        elif operator.type == OpType.Sum:
            sum = operator_instance.Reduce.remote(actor_id, operator, input,
                                                  output)
            # Register target handle at state actor
            state_actor = operator.state_actor
            if state_actor is not None:
                state_actor.register_target.remote(sum)
            # Register own handle
            sum.register_handle.remote(sum)
            return sum.start.remote()
        elif operator.type == OpType.Sink:
            pass
        elif operator.type == OpType.Inspect:
            inspect = operator_instance.Inspect.remote(actor_id, operator,
                                                       input, output)
            inspect.register_handle.remote(inspect)
            return inspect.start.remote()
        elif operator.type == OpType.ReadTextFile:
            # TODO (john): Colocate the source with the input file
            read = operator_instance.ReadTextFile.remote(
                actor_id, operator, input, output)
            read.register_handle.remote(read)
            return read.start.remote()
        else:  # TODO (john): Add support for other types of operators
            sys.exit("Unrecognized or unsupported {} operator type.".format(
                operator.type))

    # Constructs and deploys a Ray actor for each instance of
    # the given operator
    def __generate_actors(self, operator, upstream_channels,
                          downstream_channels):
        """Generates one actor for each instance of the given logical
        operator.

        Attributes:
            operator (Operator): The logical operator metadata.
            upstream_channels (list): A list of all upstream channels for
            all instances of the operator.
            downstream_channels (list): A list of all downstream channels
            for all instances of the operator.
        """
        num_instances = operator.num_instances
        logger.info("Generating {} actors of type {}...".format(
            num_instances, operator.type))
        in_channels = upstream_channels.pop(
            operator.id) if upstream_channels else []
        handles = []
        for i in range(num_instances):
            # Collect input and output channels for the particular instance
            ip = [
                channel for channel in in_channels
                if channel.dst_instance_id == i
            ] if in_channels else []
            op = [
                channel for channels_list in downstream_channels.values()
                for channel in channels_list if channel.src_instance_id == i
            ]
            log = "Constructed {} input and {} output channels "
            log += "for the {}-th instance of the {} operator."
            logger.debug(log.format(len(ip), len(op), i, operator.type))
            input_gate = DataInput(ip)
            output_gate = DataOutput(op, operator.partitioning_strategies)
            handle = self.__generate_actor(i, operator, input_gate,
                                           output_gate)
            if handle:
                handles.append(handle)
        return handles

    # Adds a channel/edge to the physical dataflow graph
    def __add_channel(self, actor_id, input, output):
        for dest_actor_id in output._destination_actor_ids():
            self.physical_topo.add_edge(actor_id, dest_actor_id)

    # Generates all required data channels between an operator
    # and its downstream operators
    def _generate_channels(self, operator):
        """Generates all output data channels
        (see: DataChannel in communication.py) for all instances of
        the given logical operator.

        The function constructs one data channel for each pair of
        communicating operator instances (instance_1,instance_2),
        where instance_1 is an instance of the given operator and instance_2
        is an instance of a direct downstream operator.

        The number of total channels generated depends on the partitioning
        strategy specified by the user.
        """
        channels = {}  # destination operator id -> channels
        strategies = operator.partitioning_strategies
        for dst_operator, p_scheme in strategies.items():
            num_dest_instances = self.operators[dst_operator].num_instances
            entry = channels.setdefault(dst_operator, [])
            if p_scheme.strategy == PStrategy.Forward:
                for i in range(operator.num_instances):
                    # ID of destination instance to connect
                    id = i % num_dest_instances
                    channel = DataChannel(self, operator.id, dst_operator, i,
                                          id)
                    entry.append(channel)
            elif p_scheme.strategy in all_to_all_strategies:
                for i in range(operator.num_instances):
                    for j in range(num_dest_instances):
                        channel = DataChannel(self, operator.id, dst_operator,
                                              i, j)
                        entry.append(channel)
            else:
                # TODO (john): Add support for other partitioning strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        return channels

    # An edge denotes a flow of data between logical operators
    # and may correspond to multiple data channels in the physical dataflow
    def _add_edge(self, source, destination):
        self.logical_topo.add_edge(source, destination)

    # Cleans the logical dataflow graph to construct and
    # deploy the physical dataflow
    def _collect_garbage(self):
        if self.topo_cleaned is True:
            return
        for node in self.logical_topo:
            self.operators[node]._clean()
        self.topo_cleaned = True

    # Sets the level of parallelism for a registered operator
    # Overwrites the environment parallelism (if set)
    def _set_parallelism(self, operator_id, level_of_parallelism):
        self.operators[operator_id].num_instances = level_of_parallelism

    # Sets the same level of parallelism for all operators in the environment
    def set_parallelism(self, parallelism):
        self.config.parallelism = parallelism

    # Sets batched queue configuration for the environment
    def set_queue_config(self, queue_config):
        self.config.queue_config = queue_config

    # Creates and registers a user-defined data source
    # TODO (john): There should be different types of sources, e.g. sources
    # reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def source(self, source):
        source_id = _generate_uuid()
        source_stream = DataStream(self, source_id)
        self.operators[source_id] = Operator(
            source_id, OpType.Source, "Source", other=source)
        return source_stream

    # Creates and registers a new data source that reads a
    # text file line by line
    # TODO (john): There should be different types of sources,
    # e.g. sources reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def read_text_file(self, filepath):
        source_id = _generate_uuid()
        source_stream = DataStream(self, source_id)
        self.operators[source_id] = Operator(
            source_id, OpType.ReadTextFile, "Read Text File", other=filepath)
        return source_stream

    # Constructs and deploys the physical dataflow
    def execute(self):
        """Deploys and executes the physical dataflow."""
        self._collect_garbage()  # Make sure everything is clean
        # TODO (john): Check if dataflow has any 'logical inconsistencies'
        # For example, if there is a forward partitioning strategy but
        # the number of downstream instances is larger than the number of
        # upstream instances, some of the downstream instances will not be
        # used at all

        # Each operator instance is implemented as a Ray actor
        # Actors are deployed in topological order, as we traverse the
        # logical dataflow from sources to sinks. At each step, data
        # producers wait for acknowledge from consumers before starting
        # generating data.
        upstream_channels = {}
        for node in nx.topological_sort(self.logical_topo):
            operator = self.operators[node]
            # Generate downstream data channels
            downstream_channels = self._generate_channels(operator)
            # Instantiate Ray actors
            handles = self.__generate_actors(operator, upstream_channels,
                                             downstream_channels)
            if handles:
                self.actor_handles.extend(handles)
            upstream_channels.update(downstream_channels)
        logger.debug("Running...")
        return self.actor_handles

    # Prints the logical dataflow graph
    def print_logical_graph(self):
        self._collect_garbage()
        logger.info("==================================")
        logger.info("======Logical Dataflow Graph======")
        logger.info("==================================")
        # Print operators in topological order
        for node in nx.topological_sort(self.logical_topo):
            downstream_neighbors = list(self.logical_topo.neighbors(node))
            logger.info("======Current Operator======")
            operator = self.operators[node]
            operator.print()
            logger.info("======Downstream Operators======")
            if len(downstream_neighbors) == 0:
                logger.info("None\n")
            for downstream_node in downstream_neighbors:
                self.operators[downstream_node].print()

    # Prints the physical dataflow graph
    def print_physical_graph(self):
        logger.info("===================================")
        logger.info("======Physical Dataflow Graph======")
        logger.info("===================================")
        # Print all data channels between operator instances
        log = "(Source Operator ID,Source Operator Name,Source Instance ID)"
        log += " --> "
        log += "(Destination Operator ID,Destination Operator Name,"
        log += "Destination Instance ID)"
        logger.info(log)
        for src_actor_id, dst_actor_id in self.physical_topo.edges:
            src_operator_id, src_instance_id = src_actor_id
            dst_operator_id, dst_instance_id = dst_actor_id
            logger.info("({},{},{}) --> ({},{},{})".format(
                src_operator_id, self.operators[src_operator_id].name,
                src_instance_id, dst_operator_id,
                self.operators[dst_operator_id].name, dst_instance_id))


# TODO (john): We also need KeyedDataStream and WindowedDataStream as
# subclasses of DataStream to prevent ill-defined logical dataflows


# A DataStream corresponds to an edge in the logical dataflow
class DataStream(object):
    """A data stream.

    This class contains all information about a logical stream, i.e. an edge
    in the logical topology. It is the main class exposed to the user.

    Attributes:
         id (UUID): The id of the stream
         env (Environment): The environment the stream belongs to.
         src_operator_id (UUID): The id of the source operator of the stream.
         dst_operator_id (UUID): The id of the destination operator of the
         stream.
         is_partitioned (bool): Denotes if there is a partitioning strategy
         (e.g. shuffle) for the stream or not (default stategy: Forward).
    """

    def __init__(self,
                 environment,
                 source_id=None,
                 dest_id=None,
                 is_partitioned=False):
        self.id = _generate_uuid()
        self.env = environment
        self.src_operator_id = source_id
        self.dst_operator_id = dest_id
        # True if a partitioning strategy for this stream exists,
        # false otherwise
        self.is_partitioned = is_partitioned

    # Generates a new stream after a data transformation is applied
    def __expand(self):
        stream = DataStream(self.env)
        assert (self.dst_operator_id is not None)
        stream.src_operator_id = self.dst_operator_id
        stream.dst_operator_id = None
        return stream

    # Assigns the partitioning strategy to a new 'open-ended' stream
    # and returns the stream. At this point, the partitioning strategy
    # is not associated with any destination operator. We expect this to
    # be done later, as we continue assembling the dataflow graph
    def __partition(self, strategy, partition_fn=None):
        scheme = PScheme(strategy, partition_fn)
        source_operator = self.env.operators[self.src_operator_id]
        new_stream = DataStream(
            self.env, source_id=source_operator.id, is_partitioned=True)
        source_operator._set_partition_strategy(new_stream.id, scheme)
        return new_stream

    # Registers the operator to the environment and returns a new
    # 'open-ended' stream. The registered operator serves as the destination
    # of the previously 'open' stream
    def __register(self, operator):
        """Registers the given logical operator to the environment and
        connects it to its upstream operator (if any).

        A call to this function adds a new edge to the logical topology.

        Attributes:
             operator (Operator): The metadata of the logical operator.
        """
        self.env.operators[operator.id] = operator
        self.dst_operator_id = operator.id
        logger.debug("Adding new dataflow edge ({},{}) --> ({},{})".format(
            self.src_operator_id,
            self.env.operators[self.src_operator_id].name,
            self.dst_operator_id,
            self.env.operators[self.dst_operator_id].name))
        # Update logical dataflow graphs
        self.env._add_edge(self.src_operator_id, self.dst_operator_id)
        # Keep track of the partitioning strategy and the destination operator
        src_operator = self.env.operators[self.src_operator_id]
        if self.is_partitioned is True:
            partitioning, _ = src_operator._get_partition_strategy(self.id)
            src_operator._set_partition_strategy(_generate_uuid(),
                                                 partitioning, operator.id)
        elif src_operator.type == OpType.KeyBy:
            # Set the output partitioning strategy to shuffle by key
            partitioning = PScheme(PStrategy.ShuffleByKey)
            src_operator._set_partition_strategy(_generate_uuid(),
                                                 partitioning, operator.id)
        else:  # No partitioning strategy has been defined - set default
            partitioning = PScheme(PStrategy.Forward)
            src_operator._set_partition_strategy(_generate_uuid(),
                                                 partitioning, operator.id)
        return self.__expand()

    # Sets the level of parallelism for an operator, i.e. its total
    # number of instances. Each operator instance corresponds to an actor
    # in the physical dataflow
    def set_parallelism(self, num_instances):
        """Sets the number of instances for the source operator of the stream.

        Attributes:
             num_instances (int): The level of parallelism for the source
             operator of the stream.
        """
        assert (num_instances > 0)
        self.env._set_parallelism(self.src_operator_id, num_instances)
        return self

    # Stream Partitioning Strategies #
    # TODO (john): Currently, only forward (default), shuffle,
    # and broadcast are supported

    # Hash-based record shuffling
    def shuffle(self):
        """Registers a shuffling partitioning strategy for the stream."""
        return self.__partition(PStrategy.Shuffle)

    # Broadcasts each record to all downstream instances
    def broadcast(self):
        """Registers a broadcast partitioning strategy for the stream."""
        return self.__partition(PStrategy.Broadcast)

    # Rescales load to downstream instances
    def rescale(self):
        """Registers a rescale partitioning strategy for the stream.

        Same as Flink's rescale (see: https://ci.apache.org/projects/flink/
        flink-docs-stable/dev/stream/operators/#physical-partitioning).
        """
        return self.__partition(PStrategy.Rescale)

    # Round-robin partitioning
    def round_robin(self):
        """Registers a round-robin partitioning strategy for the stream."""
        return self.__partition(PStrategy.RoundRobin)

    # User-defined partitioning
    def partition(self, partition_fn):
        """Registers a user-defined partitioning strategy for the stream.

        Attributes:
             partition_fn (function): The user-defined partitioning function.
        """
        return self.__partition(PStrategy.Custom, partition_fn)

    #   Data Trasnformations   #
    # TODO (john): Expand set of supported operators.
    # TODO (john): To support event-time windows we need a mechanism for
    # generating and processing watermarks

    # Registers map operator to the environment
    def map(self, map_fn, name="Map"):
        """Applies a map operator to the stream.

        Attributes:
             map_fn (function): The user-defined logic of the map.
        """
        op = Operator(
            _generate_uuid(),
            OpType.Map,
            name,
            map_fn,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers flatmap operator to the environment
    def flat_map(self, flatmap_fn):
        """Applies a flatmap operator to the stream.

        Attributes:
             flatmap_fn (function): The user-defined logic of the flatmap
             (e.g. split()).
        """
        op = Operator(
            _generate_uuid(),
            OpType.FlatMap,
            "FlatMap",
            flatmap_fn,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers keyBy operator to the environment
    # TODO (john): This should returned a KeyedDataStream
    def key_by(self, key_selector):
        """Applies a key_by operator to the stream.

        Attributes:
             key_attribute_index (int): The index of the key attributed
             (assuming tuple records).
        """
        op = Operator(
            _generate_uuid(),
            OpType.KeyBy,
            "KeyBy",
            other=key_selector,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers Reduce operator to the environment
    def reduce(self, reduce_fn):
        """Applies a rolling sum operator to the stream.

        Attributes:
             sum_attribute_index (int): The index of the attribute to sum
             (assuming tuple records).
        """
        op = Operator(
            _generate_uuid(),
            OpType.Reduce,
            "Sum",
            reduce_fn,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers Sum operator to the environment
    def sum(self, attribute_selector, state_keeper=None):
        """Applies a rolling sum operator to the stream.

        Attributes:
             sum_attribute_index (int): The index of the attribute to sum
             (assuming tuple records).
        """
        op = Operator(
            _generate_uuid(),
            OpType.Sum,
            "Sum",
            _sum,
            other=attribute_selector,
            state_actor=state_keeper,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers window operator to the environment.
    # This is a system time window
    # TODO (john): This should return a WindowedDataStream
    def time_window(self, window_width_ms):
        """Applies a system time window to the stream.

        Attributes:
             window_width_ms (int): The length of the window in ms.
        """
        op = Operator(
            _generate_uuid(),
            OpType.TimeWindow,
            "TimeWindow",
            num_instances=self.env.config.parallelism,
            other=window_width_ms)
        return self.__register(op)

    # Registers filter operator to the environment
    def filter(self, filter_fn):
        """Applies a filter to the stream.

        Attributes:
             filter_fn (function): The user-defined filter function.
        """
        op = Operator(
            _generate_uuid(),
            OpType.Filter,
            "Filter",
            filter_fn,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # TODO (john): Registers window join operator to the environment
    def window_join(self, other_stream, join_attribute, window_width):
        op = Operator(
            _generate_uuid(),
            OpType.WindowJoin,
            "WindowJoin",
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers inspect operator to the environment
    def inspect(self, inspect_logic):
        """Inspects the content of the stream.

        Attributes:
             inspect_logic (function): The user-defined inspect function.
        """
        op = Operator(
            _generate_uuid(),
            OpType.Inspect,
            "Inspect",
            inspect_logic,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers sink operator to the environment
    # TODO (john): A sink now just drops records but it should be able to
    # export data to other systems
    def sink(self):
        """Closes the stream with a sink operator."""
        op = Operator(
            _generate_uuid(),
            OpType.Sink,
            "Sink",
            num_instances=self.env.config.parallelism)
        return self.__register(op)
