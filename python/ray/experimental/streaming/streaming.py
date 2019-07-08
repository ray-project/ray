from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
import time
import uuid
from collections import defaultdict

import networkx as nx

import ray
import ray.experimental.streaming.benchmarks.utils as utils
from ray.experimental.streaming.communication import DataChannel, DataInput
from ray.experimental.streaming.communication import DataOutput, QueueConfig
import ray.experimental.streaming.operator as operator
from ray.experimental.streaming.operator import OpType
from ray.experimental.streaming.operator import PScheme, PStrategy
import ray.experimental.streaming.operator_instance as operator_instance

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Generates UUIDs
def _generate_uuid():
    return str(uuid.uuid4())

# Rolling sum's logic
def _sum(value_1, value_2):
    return value_1 + value_2

# Partitioning strategies that require all-to-all connectivity
all_to_all_strategies = [
    PStrategy.Shuffle, PStrategy.ShuffleByKey, PStrategy.Custom,
    PStrategy.Broadcast, PStrategy.RoundRobin
]

# Source types
# TODO (john): Add more
source_types = [OpType.Source]


# Environment configuration
class Config(object):
    """Environment configuration.

    This class includes all information about the configuration of the
    streaming environment.

    Attributes:
         queue_config (QueueConfig): Virtual Queue configuration
         (see: communication.py)
         A virtual queue configuration includes the max queue size in number
         of batches, the size of each batch (in number of records), and the
         batch flush timeout
         parallelism (int): The number of isntances (actors) for each logical
         dataflow operator (default: 1)
    """

    def __init__(self, parallelism=1, micro_batch_api=False, logging=False):
        # Virtual queue configuration
        self.queue_config = QueueConfig()
        # Dataflow parallelism (can be overwritten at the operator level)
        self.parallelism = parallelism
        # Micro-batch API
        self.use_micro_batch_api = micro_batch_api
        # Logging flag
        self.logging = logging
        # ...

# A handle to the physical dataflow that is being executed
# It is returned by Environment.execute()
class PhysicalDataflow(object):
    """A physical dataflow handle.

    This class provides access to the physical dataflow, including
    access to operator states, progress tracking, etc.
    """

    def __init__(self):
        # A mapping from operator ids/names to the list
        # of actor handles executing the operators
        self.actor_handles = {}
        # A mapping from operator ids to operator metadata
        self.operator_metadata = {}
        # Handle of the actor monitoring the
        # progress of the dataflow execution
        self.monitoring_actor = None
        # A mapping from operator names to ids
        self.names_to_ids = {}

    # Adds operator metadata to the dictionary and checks for duplicates
    def __register_name(self, operator_id, name):
        id = self.names_to_ids.setdefault(name, operator_id)
        # Each operator must have a unique name and id
        assert id == operator_id, (id, operator_id)

    # Adds an actor handle to the dictionary
    def _register_handle(self, actor_id, handle):
        operator_id, instance_id = actor_id
        entry = self.actor_handles.setdefault(operator_id, [None]*1)
        # Expand list if needed
        slots_to_add = instance_id - len(entry) + 1
        if slots_to_add > 0:
            for _ in range(slots_to_add):
                entry.append(None)
        # Local ids of operator instances are in 0..number_of_instances
        entry[instance_id] = handle

    # Adds operator metadata to the dictionary
    def _register_metadata(self, operator_id, metadata):
        self.operator_metadata.setdefault(operator_id, metadata)
        self.__register_name(operator_id, metadata.name)

    # Returns the handles of the actors executing the instances of the
    # operator or None if operator id or name does not exist
    def actor_handles(self, operator_id_or_name):
        self.actor_handles.get(operator_id_or_name)

    # Returns the id of the operator or None if the name does not exist
    def operator_id(self, operator_name):
        return self.names_to_ids.get(operator_name)

    # Returns all dataflow operator ids
    def operator_ids(self):
        keys_and_names = set(self.actor_handles.keys())
        names = set(self.names_to_ids.keys())
        return list(keys_and_names - names)

    # Returns all dataflow operator names
    def operator_names(self):
        return list(self.names_to_ids.keys())

    # Returns the name of the operator
    def name_of(self, operator_id):
        return self.operator_metadata[operator_id].name

    # Returns a list of futures representing the local
    # state of each actor executing the given operator.
    # This is equivalent to manually retrieving the actor handles
    # via actor_handles() and then call state() on each handle
    def state_of(self, operator_id_or_name):
        state = []  # One state object per operator instance
        actor_handles = self.actor_handles.get(operator_id_or_name)
        assert actor_handles is not None
        for actor_handle in actor_handles:
            state.append(actor_handle.state.remote())
        return state

    # Returns a list of futures representing the logged
    # rates for all actors executing the given operator
    def logs_of(self, operator_id_or_name):
        logs = []  # One log object per operator instance
        actor_handles = self.actor_handles.get(operator_id_or_name)
        assert actor_handles is not None
        for actor_handle in actor_handles:
            logs.append(actor_handle.logs.remote())
        return logs

    # Returns a future representing the execution's status upon termination
    def termination_status(self):
        return self.monitoring_actor.all_exit_signals.remote()


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

    def __init__(self, checkpoint_dir=None, config=Config()):
        self.logical_topo = nx.DiGraph()  # DAG
        self.physical_topo = nx.DiGraph()  # DAG
        self.operators = {}  # operator id --> operator object
        self.config = config  # Environment's configuration
        self.topo_cleaned = False
        # A handle to the running dataflow
        self.physical_dataflow = PhysicalDataflow()
        self.checkpoint_dir = checkpoint_dir

    # Constructs and deploys an actor of a specific type
    def __generate_actor(self, instance_id, operator, input, output):
        """Generates an actor that will execute an instance of
        the logical operator

        Attributes:
            instance_id (str(UUID)): The id of the instance the actor will
            execute.
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
        actor_handle = None
        args = [actor_id, operator, input, output, self.checkpoint_dir]
        if operator.type == OpType.Source:
            node_id = operator.placement[instance_id]
            logger.info("Placing source {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.Source._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif operator.type == OpType.Map:
            node_id = operator.placement[instance_id]
            logger.info("Placing map {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.Map._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif operator.type == OpType.FlatMap:
            node_id = operator.placement[instance_id]
            logger.info("Placing flatmap {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.FlatMap._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif operator.type == OpType.Union:
            # Other input streams are not needed any more.
            # Discard them to avoid serialization errors due to
            # recursive references between DataStream and Environment
            del operator.other_inputs[:]
            node_id = operator.placement[instance_id]
            logger.info("Placing union {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.Union._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif ((operator.type == OpType.Reduce) or
              (operator.type ==  OpType.Sum)):
            node_id = operator.placement[instance_id]
            logger.info("Placing reduce {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.Reduce._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif operator.type == OpType.KeyBy:
            node_id = operator.placement[instance_id]
            logger.info("Placing keyBy {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.KeyBy._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        elif operator.type == OpType.Sink:
            node_id = operator.placement[instance_id]
            logger.info("Placing sink {} at {}.".format(actor_id, node_id))
            actor_handle = operator_instance.Sink._remote(args=args,
                                                    kwargs=None,
                                                    resources={node_id: 1})
        else:  # TODO (john): Add support for other types of operators
            message = "Unrecognized or unsupported operator type: {}"
            raise Exception(message.format(operator.type))

        # Register current actor handle as destination to upstream
        # actors so that upstream actors can invoke tasks on it
        for channel in input.input_channels:
            operator_id, _ = channel.src_actor_id
            upstream_actor_handles = self.physical_dataflow.actor_handles[
                                                                operator_id]
            for handle in upstream_actor_handles:
                destination_handle = ray.put(actor_handle)
                # Make sure the handle is registered before proceeding
                ray.get(handle._register_destination_handle.remote(
                                            destination_handle, channel.id))
        # Register own handle (used by actor for scheduling itself)
        ray.get(actor_handle._register_handle.remote(actor_handle))
        # Register operator to the physical dataflow
        self.physical_dataflow._register_handle(actor_id, actor_handle)
        self.physical_dataflow._register_metadata(operator.id, operator)
        assert actor_handle is not None
        return actor_handle

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
            operator.id) if operator.id in upstream_channels else []
        handles = []  # The actor handles for the operator instances
        for i in range(num_instances):
            # Collect input and output channels for the particular instance
            input_channels = [
                channel for channel in in_channels
                if channel.dst_instance_id == i
            ] if in_channels else []
            outout_channels = [
                channel for channels_list in downstream_channels.values()
                for channel in channels_list if channel.src_instance_id == i
            ]
            log = "Constructed {} input and {} output channels "
            log += "for the {}-th instance of the {} operator."
            logger.debug(log.format(len(ip), len(op), i, operator.type))
            input_gate = DataInput(input_channels)
            output_gate = DataOutput(outout_channels,
                                     operator.partitioning_strategies)
            handle = self.__generate_actor(i, operator, input_gate,
                                           output_gate)
            handles.append(handle)
        return handles

    # Adds a channel/edge to the physical dataflow graph
    def __add_channel(self, actor_id, input, output):
        for dest_actor_id in output._destination_actor_ids():
            self.physical_topo.add_edge(actor_id, dest_actor_id)

    # Generates all required data channels between an operator and
    # its downstream operators
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
        env_config = self.config
        channels = {}  # destination operator id -> channels
        strategies = operator.partitioning_strategies
        for dst_operator, p_scheme in strategies.items():
            num_dest_instances = self.operators[dst_operator].num_instances
            entry = channels.setdefault(dst_operator, [])
            if p_scheme.strategy in all_to_all_strategies:
                for i in range(operator.num_instances):
                    for j in range(num_dest_instances):
                        channel = DataChannel(env_config, operator.id,
                                              dst_operator, i, j)
                        entry.append(channel)
            elif p_scheme.strategy == PStrategy.Rescale:
                channels_per_instance = math.trunc(math.ceil(
                                num_dest_instances / operator.num_instances))
                index = 0
                for i in range(operator.num_instances):
                    for _ in range(channels_per_instance):
                        if index == num_dest_instances:
                            index = i % num_dest_instances
                        channel = DataChannel(env_config, operator.id,
                                              dst_operator, i, index)
                        entry.append(channel)
                        index += 1
            else:
                message = "Unrecognized or unsupported partitioning strategy."
                raise Exception(message)
        return channels

    # An edge denotes a flow of data between logical operators and may
    # correspond to multiple data channels in the physical dataflow
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
    # by overwriting the streaming environment parallelism
    def _set_parallelism(self, operator_id, level_of_parallelism):
        self.operators[operator_id].num_instances = level_of_parallelism

    # Sets the same level of parallelism for all operators in the environment
    def set_parallelism(self, parallelism):
        self.config.parallelism = parallelism

    # Sets virtual queue configuration for the environment
    def set_queue_config(self, queue_config):
        self.config.queue_config = queue_config

    # Sets the same level of parallelism for all operators in the environment
    def use_microbatch_api(self):
        self.config.use_micro_batch_api = True

    # Enables actor logging
    def enable_logging(self):
        self.config.logging = True

    # Creates and registers a user-defined data source
    def source(self, source_object, watermark_interval=0,
               name="Source_"+_generate_uuid(),
               placement=None):
        source_id = _generate_uuid()
        source_stream = DataStream(self, source_id)
        self.operators[source_id] = operator.CustomSourceOperator(source_id,
                                             OpType.Source,
                                             source_object,
                                             watermark_interval,
                                             name,
                                             logging=self.config.logging,
                                             placement=placement)
        return source_stream

    # Constructs and deploys the physical dataflow
    def execute(self):
        """Deploys and executes the physical dataflow."""
        # Step 0: Clean logical dataflow
        self._collect_garbage()  # Make sure everything is clean
        # TODO (john): Check if dataflow has any 'logical inconsistencies'
        # For example, if there is a forward partitioning strategy but
        # the number of downstream instances is larger than the number of
        # upstream instances, some of the downstream instances will not be
        # used at all


        logger.debug("Deploying and executing the dataflow...")
        # # Step 1: Deploy actors. Each operator instance is implemented as a
        # Ray actor. Actors are deployed in topological order, as we traverse
        # the logical dataflow from sources to sinks. Sources are started at
        # the end of the dataflow construction.
        all_handles = []
        upstream_channels = {}
        for node in nx.topological_sort(self.logical_topo):
            operator = self.operators[node]
            # Generate downstream data channels
            downstream_channels = self._generate_channels(operator)
            # Instantiate Ray actors with their input and output channels
            handles = self.__generate_actors(operator, upstream_channels,
                                             downstream_channels)
            all_handles.extend(handles)
            # Update upstream channels
            for destination, channels in downstream_channels.items():
                existing_channels = upstream_channels.setdefault(destination,
                                                                 channels)
                # In case there are multi-input operators in the dataflow,
                # some channels might exist already as we traverse the graph
                if existing_channels != channels:
                    value.extend(channels)

        # Step 2: Start and register monitoring actor to the physical dataflow
        first_node_id = utils.get_cluster_node_ids()[0]
        logger.info("Placing progress monitor at {}.".format(first_node_id))
        monitoring_actor = operator_instance.ProgressMonitor._remote(
                                            args=[all_handles], kwargs=None,
                                            resources={first_node_id: 1})
        self.physical_dataflow.monitoring_actor = monitoring_actor

        logger.info("Starting sources...")
        # Step 3: Start data sources
        source_handles = []  # The source actor handles
        for entry in self.physical_dataflow.actor_handles.items():
            operator_id, actor_handles = entry
            operator = self.physical_dataflow.operator_metadata[operator_id]
            operator_type = operator.type
            # Keep source handles for now to start sources in the end
            if operator_type in source_types:
                source_handles.extend(actor_handles)
        #  Start spinning
        for source_handle in source_handles:
            _ = source_handle.start.remote()

        # Step 4: Update dictionary in physical topology so that
        # users can lookup operators using either an id or a name
        operator_ids = self.physical_dataflow.actor_handles.items()
        operator_names = {}  # name -> actor handles
        for operator_id, handles in operator_ids:
            metadata = self.physical_dataflow.operator_metadata[operator_id]
            operator_name = metadata.name
            operator_names.setdefault(operator_name, handles)
        self.physical_dataflow.actor_handles.update(operator_names)

        # Return handle to physical dataflow
        return self.physical_dataflow

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


# A DataStream corresponds to an edge in the logical dataflow
# TODO (john): We need KeyedDataStream and WindowedDataStream as
# subclasses of DataStream to prevent ill-defined logical dataflows
class DataStream(object):
    """A data stream.

    This class contains all information about a logical stream, i.e. an edge
    in the logical topology. It is the main class exposed to the user.

    Attributes:
         id (str(UUID)): The id of the stream
         env (Environment): The environment the stream belongs to.
         src_operator_id (str(UUID)): The id of the source operator of the
         stream.
         dst_operator_id (str(UUID)): The id of the destination operator of
         the stream.
         is_partitioned (bool): Denotes if there is a partitioning strategy
         (e.g. shuffle) for the stream or not (default stategy: Rescale).
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
        connects it to its upstream operator(s) (if any).

        A call to this function adds new edges to the logical topology.

        Attributes:
             operator (Operator): The metadata of the logical operator.
        """
        self.env.operators[operator.id] = operator
        input_streams = [self]
        # TODO (john): Add support for other multiple-input operators
        if operator.type == OpType.Union:
            input_streams.extend(operator.other_inputs)
        elif operator.type == OpType.Join:
            input_streams.append(operator.right_input)
        # ...
        for input_stream in input_streams:
            input_stream.dst_operator_id = operator.id
            logger.debug(
                "Adding new dataflow edge ({},{}) --> ({},{})".format(
                input_stream.src_operator_id,
                input_stream.env.operators[input_stream.src_operator_id].name,
                input_stream.dst_operator_id,
                input_stream.env.operators[input_stream.dst_operator_id].name)
                )
            # Update logical dataflow graph
            input_stream.env._add_edge(input_stream.src_operator_id,
                                                input_stream.dst_operator_id)
            # Keep track of the partitioning strategy and destination operator
            src_operator = input_stream.env.operators[
                                                input_stream.src_operator_id]
            if input_stream.is_partitioned is True:
                partitioning, _ = src_operator._get_partition_strategy(
                                                            input_stream.id)
                src_operator._set_partition_strategy(_generate_uuid(),
                                                    partitioning, operator.id)
            elif src_operator.type == OpType.KeyBy:
                # Set the output partitioning strategy to shuffle by key
                partitioning = PScheme(PStrategy.ShuffleByKey)
                src_operator._set_partition_strategy(_generate_uuid(),
                                                    partitioning, operator.id)
            else:  # No partitioning strategy has been defined - set default
                partitioning = PScheme(PStrategy.Rescale)
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

    # Registers Map operator to the environment
    def map(self, map_fn, name="Map_"+_generate_uuid(),
            placement=None):
        """Applies a map operator to the stream.

        Attributes:
             map_fn (function): The user-defined logic of the map.
             name (str): The name of the map operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        op = operator.Operator(
            _generate_uuid(),
            OpType.Map,
            name,
            map_fn,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)

    # Registers Flatmap operator to the environment
    def flat_map(self, flatmap_logic, name="FlatMap_"+_generate_uuid(),
                 placement=None):
        """Applies a flatmap operator to the stream.

        Attributes:
             flatmap_fn (function): The user-defined logic of the flatmap
             (e.g. split()).
             name (str): The name of the flatmap operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        op = operator.Operator(
            _generate_uuid(),
            OpType.FlatMap,
            name,
            flatmap_logic,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)

    # Registers KeyBy operator to the environment
    # TODO (john): This should returned a KeyedDataStream
    # A stream can be partitioned on a key either within the upstream operator
    # or by introducing a pipelined KeyBy operator. The latter transforms the
    # input stream of records to a new stream of tuples (key, record).
    def key_by(self, key_selector, name="KeyBy_"+_generate_uuid(),
               placement=None):
        """Applies a key_by operator to the stream.

        Attributes:
             key_attribute_index (int): The index of the key attributed
             (assuming tuple records).
             name (str): The name of the keyBy operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        op = operator.KeyByOperator(
            _generate_uuid(),
            OpType.KeyBy,
            key_selector,
            name,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)

    # Registers Reduce operator to the environment
    def reduce(self, reduce_fn, name="Reduce_"+_generate_uuid(),
               placement=None):
        """Applies a reduce operator to the stream.

        Attributes:
             reduce_fn (function): The user-defined reduce function.
             name (str): The name of the reduce operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        op = operator.Operator(
            _generate_uuid(),
            OpType.Reduce,
            name,
            reduce_fn,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)

    # Registers Sum operator to the environment
    def sum(self, attribute_selector, name="Sum_"+_generate_uuid(),
            placement=None):
        """Applies a rolling sum operator to the stream.

        Attributes:
             sum_attribute_index (int): The index of the attribute to sum
             (assuming tuple records).
             name (str): The name of the sum operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        op = operator.SumOperator(
            _generate_uuid(),
            OpType.Sum,
            attribute_selector,
            name,
            _sum,  # A function that takes two values and returns their sum
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)

    # Registers union operator to the environment
    def union(self, other_inputs, name="Union_"+_generate_uuid(),
              placement=None):
        """Unions the stream with one or more other streams.

        Attributes:
             other_streams list(DataStream): The list of streams to union.
             name (str): The name of the union operator
             placement list(str): A mapping from local instance ids in
             [0...num_instances) to cluster node ids
        """
        assert isinstance(other_inputs, list), other_inputs
        op = operator.UnionOperator(
            _generate_uuid(),
            OpType.Union,
            other_inputs,
            name,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)


    # Registers a custom sink operator to the environment
    def sink(self, sink_object, name="Sink_"+_generate_uuid(),
             placement=None):
        """Closes the stream with a custom sink operator.

        Attributes:
            sink_object (object): The sink class with the user-defined logic
            name (str): The name of the sink operator
            placement list(str): A mapping from local instance ids in
            [0...num_instances) to cluster node ids
        """
        op = operator.CustomSinkOperator(
            _generate_uuid(),
            OpType.Sink,
            sink_object,
            name,
            num_instances=self.env.config.parallelism,
            logging=self.env.config.logging,
            placement=placement)
        return self.__register(op)
