import logging
import pickle
import sys
import time

import networkx as nx
import ray
import ray.streaming.processor as processor
import ray.streaming.runtime.transfer as transfer
from ray.streaming.communication import DataChannel
from ray.streaming.config import Config
from ray.streaming.jobworker import JobWorker
from ray.streaming.operator import Operator, OpType
from ray.streaming.operator import PScheme, PStrategy

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Rolling sum's logic
def _sum(value_1, value_2):
    return value_1 + value_2


# Partitioning strategies that require all-to-all instance communication
all_to_all_strategies = [
    PStrategy.Shuffle, PStrategy.ShuffleByKey, PStrategy.Broadcast,
    PStrategy.RoundRobin
]


# Environment configuration
class Conf:
    """Environment configuration.

    This class includes all information about the configuration of the
    streaming environment.
    """

    def __init__(self, parallelism=1, channel_type=Config.MEMORY_CHANNEL):
        self.parallelism = parallelism
        self.channel_type = channel_type
        # ...


class ExecutionGraph:
    def __init__(self, env):
        self.env = env
        self.physical_topo = nx.DiGraph()  # DAG
        # Handles to all actors in the physical dataflow
        self.actor_handles = []
        # (op_id, op_instance_index) -> ActorID
        self.actors_map = {}
        # execution graph build time: milliseconds since epoch
        self.build_time = 0
        self.task_id_counter = 0
        self.task_ids = {}
        self.input_channels = {}  # operator id -> input channels
        self.output_channels = {}  # operator id -> output channels

    # Constructs and deploys a Ray actor of a specific type
    # TODO (john): Actor placement information should be specified in
    # the environment's configuration
    def __generate_actor(self, instance_index, operator, input_channels,
                         output_channels):
        """Generates an actor that will execute a particular instance of
        the logical operator

        Attributes:
            instance_index: The index of the instance the actor will execute.
            operator: The metadata of the logical operator.
            input_channels: The input channels of the instance.
            output_channels The output channels of the instance.
        """
        worker_id = (operator.id, instance_index)
        # Record the physical dataflow graph (for debugging purposes)
        self.__add_channel(worker_id, output_channels)
        # Note direct_call only support pass by value
        return JobWorker._remote(
            args=[worker_id, operator, input_channels, output_channels],
            is_direct_call=True)

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
        handles = []
        for i in range(num_instances):
            # Collect input and output channels for the particular instance
            ip = [c for c in upstream_channels if c.dst_instance_index == i]
            op = [c for c in downstream_channels if c.src_instance_index == i]
            log = "Constructed {} input and {} output channels "
            log += "for the {}-th instance of the {} operator."
            logger.debug(log.format(len(ip), len(op), i, operator.type))
            handle = self.__generate_actor(i, operator, ip, op)
            if handle:
                handles.append(handle)
                self.actors_map[(operator.id, i)] = handle
        return handles

    # Adds a channel/edge to the physical dataflow graph
    def __add_channel(self, actor_id, output_channels):
        for c in output_channels:
            dest_actor_id = (c.dst_operator_id, c.dst_instance_index)
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
            num_dest_instances = self.env.operators[dst_operator].num_instances
            entry = channels.setdefault(dst_operator, [])
            if p_scheme.strategy == PStrategy.Forward:
                for i in range(operator.num_instances):
                    # ID of destination instance to connect
                    id = i % num_dest_instances
                    qid = self._gen_str_qid(operator.id, i, dst_operator, id)
                    c = DataChannel(operator.id, i, dst_operator, id, qid)
                    entry.append(c)
            elif p_scheme.strategy in all_to_all_strategies:
                for i in range(operator.num_instances):
                    for j in range(num_dest_instances):
                        qid = self._gen_str_qid(operator.id, i, dst_operator,
                                                j)
                        c = DataChannel(operator.id, i, dst_operator, j, qid)
                        entry.append(c)
            else:
                # TODO (john): Add support for other partitioning strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        return channels

    def _gen_str_qid(self, src_operator_id, src_instance_index,
                     dst_operator_id, dst_instance_index):
        from_task_id = self.env.execution_graph.get_task_id(
            src_operator_id, src_instance_index)
        to_task_id = self.env.execution_graph.get_task_id(
            dst_operator_id, dst_instance_index)
        return transfer.ChannelID.gen_id(from_task_id, to_task_id,
                                         self.build_time)

    def _gen_task_id(self):
        task_id = self.task_id_counter
        self.task_id_counter += 1
        return task_id

    def get_task_id(self, op_id, op_instance_id):
        return self.task_ids[(op_id, op_instance_id)]

    def get_actor(self, op_id, op_instance_id):
        return self.actors_map[(op_id, op_instance_id)]

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
            src_operator_id, src_instance_index = src_actor_id
            dst_operator_id, dst_instance_index = dst_actor_id
            logger.info("({},{},{}) --> ({},{},{})".format(
                src_operator_id, self.env.operators[src_operator_id].name,
                src_instance_index, dst_operator_id,
                self.env.operators[dst_operator_id].name, dst_instance_index))

    def build_graph(self):
        self.build_channels()

        # to support cyclic reference serialization
        try:
            ray.register_custom_serializer(Environment, use_pickle=True)
            ray.register_custom_serializer(ExecutionGraph, use_pickle=True)
            ray.register_custom_serializer(OpType, use_pickle=True)
            ray.register_custom_serializer(PStrategy, use_pickle=True)
        except Exception:
            # local mode can't use pickle
            pass

        # Each operator instance is implemented as a Ray actor
        # Actors are deployed in topological order, as we traverse the
        # logical dataflow from sources to sinks.
        for node in nx.topological_sort(self.env.logical_topo):
            operator = self.env.operators[node]
            # Instantiate Ray actors
            handles = self.__generate_actors(
                operator, self.input_channels.get(node, []),
                self.output_channels.get(node, []))
            if handles:
                self.actor_handles.extend(handles)

    def build_channels(self):
        self.build_time = int(time.time() * 1000)
        # gen auto-incremented unique task id for every operator instance
        for node in nx.topological_sort(self.env.logical_topo):
            operator = self.env.operators[node]
            for i in range(operator.num_instances):
                operator_instance_id = (operator.id, i)
                self.task_ids[operator_instance_id] = self._gen_task_id()
        channels = {}
        for node in nx.topological_sort(self.env.logical_topo):
            operator = self.env.operators[node]
            # Generate downstream data channels
            downstream_channels = self._generate_channels(operator)
            channels[node] = downstream_channels
        # op_id -> channels
        input_channels = {}
        output_channels = {}
        for op_id, all_downstream_channels in channels.items():
            for dst_op_channels in all_downstream_channels.values():
                for c in dst_op_channels:
                    dst = input_channels.setdefault(c.dst_operator_id, [])
                    dst.append(c)
                    src = output_channels.setdefault(c.src_operator_id, [])
                    src.append(c)
        self.input_channels = input_channels
        self.output_channels = output_channels


# The execution environment for a streaming job
class Environment:
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

    def __init__(self, config=Conf()):
        self.logical_topo = nx.DiGraph()  # DAG
        self.operators = {}  # operator id --> operator object
        self.config = config  # Environment's configuration
        self.topo_cleaned = False
        self.operator_id_counter = 0
        self.execution_graph = None  # set when executed

    def gen_operator_id(self):
        op_id = self.operator_id_counter
        self.operator_id_counter += 1
        return op_id

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

    # Creates and registers a user-defined data source
    # TODO (john): There should be different types of sources, e.g. sources
    # reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def source(self, source):
        source_id = self.gen_operator_id()
        source_stream = DataStream(self, source_id)
        self.operators[source_id] = Operator(
            source_id, OpType.Source, processor.Source, "Source", logic=source)
        return source_stream

    # Creates and registers a new data source that reads a
    # text file line by line
    # TODO (john): There should be different types of sources,
    # e.g. sources reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def read_text_file(self, filepath):
        source_id = self.gen_operator_id()
        source_stream = DataStream(self, source_id)
        self.operators[source_id] = Operator(
            source_id,
            OpType.ReadTextFile,
            processor.ReadTextFile,
            "Read Text File",
            other=filepath)
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

        self.execution_graph = ExecutionGraph(self)
        self.execution_graph.build_graph()
        logger.info("init...")
        # init
        init_waits = []
        for actor_handle in self.execution_graph.actor_handles:
            init_waits.append(actor_handle.init.remote(pickle.dumps(self)))
        for wait in init_waits:
            assert ray.get(wait) is True
        logger.info("running...")
        # start
        exec_handles = []
        for actor_handle in self.execution_graph.actor_handles:
            exec_handles.append(actor_handle.start.remote())

        return exec_handles

    def wait_finish(self):
        for actor_handle in self.execution_graph.actor_handles:
            while not ray.get(actor_handle.is_finished.remote()):
                time.sleep(1)

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


# TODO (john): We also need KeyedDataStream and WindowedDataStream as
# subclasses of DataStream to prevent ill-defined logical dataflows


# A DataStream corresponds to an edge in the logical dataflow
class DataStream:
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
    stream_id_counter = 0

    def __init__(self,
                 environment,
                 source_id=None,
                 dest_id=None,
                 is_partitioned=False):
        self.env = environment
        self.id = DataStream.stream_id_counter
        DataStream.stream_id_counter += 1
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
            src_operator._set_partition_strategy(self.id, partitioning,
                                                 operator.id)
        elif src_operator.type == OpType.KeyBy:
            # Set the output partitioning strategy to shuffle by key
            partitioning = PScheme(PStrategy.ShuffleByKey)
            src_operator._set_partition_strategy(self.id, partitioning,
                                                 operator.id)
        else:  # No partitioning strategy has been defined - set default
            partitioning = PScheme(PStrategy.Forward)
            src_operator._set_partition_strategy(self.id, partitioning,
                                                 operator.id)
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
            self.env.gen_operator_id(),
            OpType.Map,
            processor.Map,
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
            self.env.gen_operator_id(),
            OpType.FlatMap,
            processor.FlatMap,
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
            self.env.gen_operator_id(),
            OpType.KeyBy,
            processor.KeyBy,
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
            self.env.gen_operator_id(),
            OpType.Reduce,
            processor.Reduce,
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
            self.env.gen_operator_id(),
            OpType.Sum,
            processor.Reduce,
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
        raise Exception("time_window is unsupported")

    # Registers filter operator to the environment
    def filter(self, filter_fn):
        """Applies a filter to the stream.

        Attributes:
             filter_fn (function): The user-defined filter function.
        """
        op = Operator(
            self.env.gen_operator_id(),
            OpType.Filter,
            processor.Filter,
            "Filter",
            filter_fn,
            num_instances=self.env.config.parallelism)
        return self.__register(op)

    # TODO (john): Registers window join operator to the environment
    def window_join(self, other_stream, join_attribute, window_width):
        op = Operator(
            self.env.gen_operator_id(),
            OpType.WindowJoin,
            processor.WindowJoin,
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
            self.env.gen_operator_id(),
            OpType.Inspect,
            processor.Inspect,
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
            self.env.gen_operator_id(),
            OpType.Sink,
            processor.Sink,
            "Sink",
            num_instances=self.env.config.parallelism)
        return self.__register(op)
