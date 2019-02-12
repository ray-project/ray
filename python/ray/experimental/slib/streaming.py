import sys
import uuid
import networkx as nx

from ray.slib.operator import *
from ray.slib.actor import *
from ray.slib.batched_queue import BatchedQueue

# Generates UUIDs
def generate_uuid():
    return uuid.uuid4()

# Environment configuration
class Config(object):
    def __init__(self,parallelism=1):
        self.queue_config = QueueConfig()
        self.parallelism = parallelism
        # ...

# The execution environment for a streaming job
class Environment(object):
    def __init__(self, config=Config()):
        self.logical_topo = nx.DiGraph()    # DAG
        self.physical_topo = nx.DiGraph()   # DAG
        self.operators = {}                 # operator id --> operator object
        self.config = config                # Environment's configuration
        self.topo_cleaned = False           # True if logical dataflow graph is ready for execution, false otherwise
        self.actor_handles = []             # Handles to all actors in the physical dataflow

    # Generates all required data channels between an operator and its downstream operators
    def __generate_channels(self, operator):
        channels = {}   # destination operator id -> channels
        for dst,s in operator.partitioning_strategies.items():
            num_dest_instances = self.operators[dst].num_instances
            entry = channels.setdefault(dst,[])
            if s.strategy == PStrategy.Forward:
                for i in range(operator.num_instances):
                    id = i % num_dest_instances # ID of destination instance
                    channel = DataChannel(self,operator.id,dst,i,id)
                    entry.append(channel)
            elif s.strategy == PStrategy.Shuffle or \
                 s.strategy == PStrategy.ShuffleByKey or \
                 s.strategy == PStrategy.Broadcast:
                for i in range(operator.num_instances):
                    for j in range(num_dest_instances):
                        channel = DataChannel(self,operator.id,dst,i,j)
                        entry.append(channel)
            else:   # TODO (john): Add support for other partitioning strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        return channels

    # Constructs and deploys a Ray actor of a specific type
    # TODO (john): Actor placement information should be specified in the environment's configuration
    def __generate_actor(self, instance_id, operator, input, output):
        actor_id = (operator.id,instance_id)
        # Record the physical dataflow graph (for debugging purposes)
        self.__add_channel(actor_id,input,output)
        # Select actor to construct
        if operator.type == OpType.Source:
            s = Source.remote(actor_id,operator,input,output)
            return s.start.remote()
        elif operator.type == OpType.Map:
            m = Map.remote(actor_id,operator,input,output)
            return m.start.remote()
        elif operator.type == OpType.FlatMap:
            fm = FlatMap.remote(actor_id,operator,input,output)
            return fm.start.remote()
        elif operator.type == OpType.Filter:
            f = Filter.remote(actor_id,operator,input,output)
            return f.start.remote()
        elif operator.type == OpType.Reduce:
            r = Reduce.remote(actor_id,operator,input,output)
            return r.start.remote()
        elif operator.type == OpType.TimeWindow:
            pass
        elif operator.type == OpType.KeyBy:
            kb = KeyBy.remote(actor_id,operator,input,output)
            return kb.start.remote()
        elif operator.type == OpType.Sum:
            s = Reduce.remote(actor_id,operator,input,output)
            return s.start.remote()
        elif operator.type == OpType.Sink:
            pass
        elif operator.type == OpType.Inspect:
            i = Inspect.remote(actor_id,operator,input,output)
            return i.start.remote()
        elif operator.type == OpType.ReadTextFile:  # TODO (john): Colocate the source with the input file
            i = ReadTextFile(actor_id,operator,input,output)
            i.start()
            return None
        else: # TODO (john): Add support for other types of operators
            sys.exit("Unrecognized or unsupported {} operator type.".format(operator.type))

    # Constructs and deploys a Ray actor for each instance of the given operator
    def __generate_actors(self, operator, upstream_channels, downstream_channels):
        num_instances = operator.num_instances
        print("Generating {} actors of type {}...".format(num_instances,operator.type))
        in_channels = upstream_channels.pop(operator.id) if upstream_channels else []
        handles = []
        for i in range(num_instances):
            # Collect input and output channels for the particular instance
            ip = [c for c in in_channels if c.dst_instance_id == i] if in_channels else []
            op = [c for clist in downstream_channels.values() for c in clist if c.src_instance_id == i]
            #print("Constructed {} input and {} output channels for the {}-th instance of the {} operator.".format(len(ip),len(op),i,operator.type))
            inpt = DataInput(ip)
            otpt = DataOutput(op,operator.partitioning_strategies)
            handle =  self.__generate_actor(i,operator,inpt,otpt)
            if handle: handles.append(handle)
        return handles

    # An edge denotes a flow of data between logical operators
    # and may correspond to multiple data channels in the physical dataflow
    def _add_edge(self, source, destination):
        self.logical_topo.add_edge(source,destination)

    # Adds a channel/edge to the physical dataflow graph
    def __add_channel(self,actor_id,input,output):
        for dest_actor_id in output._destination_actor_ids():
            self.physical_topo.add_edge(actor_id,dest_actor_id)

    # Sets the level of parallelism for a registered operator
    # Overwrites the environment parallelism (if set)
    def _set_parallelism(self, operator_id, level_of_parallelism):
        self.operators[operator_id].num_instances = level_of_parallelism

    # Sets the same level of parallelism for all operators in the environment
    def set_parallelism(self, parallelism):
        self.config.parallelism = parallelism

    # Creates and registers a new data source
    # TODO (john): There should be different types of sources, e.g. sources reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def new_source(self, _filepath):
        source_id = generate_uuid()
        source_stream = DataStream(self,source_id)
        self.operators[source_id] = Operator(source_id,OpType.Source,"Source")
        return source_stream

    # Creates and registers a new data source that reads a text file line by line
    # TODO (john): There should be different types of sources, e.g. sources reading from Kafka, text files, etc.
    # TODO (john): Handle case where environment parallelism is set
    def read_text_file(self, filepath):
        source_id = generate_uuid()
        source_stream = DataStream(self,source_id)
        self.operators[source_id] = Operator(source_id,OpType.ReadTextFile,"Read Text File", other=filepath)
        return source_stream

    # Constructs and deploys the physical dataflow
    def execute(self):
        self.collect_garbage()  # Make sure everything is clean
        # TODO (john): Check if dataflow has any 'logical inconsistencies'
        # For example, if there is a forward partitioning strategy but the number
        # of downstream instances is larger than the number of upstream instances,
        # some of the downstream instances will not be used at all

        # Each operator instance is implemented as a Ray actor
        # Actors are deployed in topological order, as we traverse the logical dataflow
        # from sources to sinks. At each step, data producers wait for acknowledge from consumers
        # before starting generating data.
        upstream_channels = {}
        for node in nx.topological_sort(self.logical_topo):
            operator = self.operators[node]
            # Generate downstream data channels
            downstream_channels = self.__generate_channels(operator)
            # Instantiate Ray actors
            handles = self.__generate_actors(operator,upstream_channels,downstream_channels)
            if handles:
                self.actor_handles.extend(handles)
            upstream_channels.update(downstream_channels)
        #self.print_physical_graph()
        # Wait until everybody returns
        print("Running...".format(len(self.actor_handles)))
        start = time.time()
        ray.get(self.actor_handles)
        print("Done.")

    # Cleans the logical dataflow graph to construct and deploy the physical dataflow
    def collect_garbage(self):
        if self.topo_cleaned == True: return
        for n in self.logical_topo: self.operators[n]._clean()
        self.topo_cleaned = True

    # Prints the logical dataflow graph
    def print_logical_graph(self):
        self.collect_garbage()
        print("==================================")
        print("======Logical Dataflow Graph======")
        print("==================================")
        # Print operators in topological order
        for node in nx.topological_sort(self.logical_topo):
            downstream_neighbors = list(self.logical_topo.neighbors(node))
            print("======Current Operator======")
            operator = self.operators[node]
            operator.print()
            print("======Downstream Operators======")
            if len(downstream_neighbors) == 0: print("None\n")
            for n in downstream_neighbors: self.operators[n].print()

    # Prints the physical dataflow graph
    def print_physical_graph(self):
        print("===================================")
        print("======Physical Dataflow Graph======")
        print("===================================")
        # Print all data channels between operator instances
        print("(Source Operator ID,Source Operator Name,Source Instance ID) --> (Destination Operator ID,Destination Operator Name,Destination Instance ID)")
        for src_actor_id,dst_actor_id in self.physical_topo.edges:
            src_op_id,src_inst_id = src_actor_id
            dst_op_id,dst_inst_id = dst_actor_id
            print("({},{},{}) --> ({},{},{})".format(src_op_id,self.operators[src_op_id].name,src_inst_id,dst_op_id,self.operators[dst_op_id].name,dst_inst_id))

# TODO (john): We also need KeyedDataStream and WindowedDataStream as subclasses of DataStream
# to prevent ill-defined logical dataflows

# A DataStream corresponds to an edge in the logical dataflow
class DataStream(object):
    def __init__(self, environment, source_id=None, dest_id=None, is_partitioned=False):
        self.id = generate_uuid()
        self.env = environment
        self.src_operator_id = source_id
        self.dest_operator_id = dest_id
        self.is_partitioned = is_partitioned    # True if a partitioning strategy for this stream exists, false otherwise

    # Generates a new stream after a data transformation is applied
    def __expand(self):
        stream = DataStream(self.env)
        assert(self.dest_operator_id!=None)
        stream.src_operator_id = self.dest_operator_id
        stream.dest_operator_id = None
        return stream

    # Assigns the partitioning strategy to a new 'open-ended' stream and returns
    # the stream. At this point, the partitioning strategy is not associated with
    # any destination operator. We expect this to be done later, as we continue
    # assembling the dataflow graph
    def __partition(self, strategy, partition_fn=None):
        scheme = PScheme(strategy, partition_fn)
        source_operator = self.env.operators[self.src_operator_id]
        new_stream = DataStream(self.env,source_id=source_operator.id,is_partitioned=True)
        source_operator._set_partition_strategy(new_stream.id, scheme)
        return new_stream

    # Registers the operator to the environment and returns a new 'open-ended' stream
    # The registered operator serves as the destination of the previously 'open' stream
    def __register(self, operator):
        self.dest_op_metadata = operator
        self.env.operators[operator.id] = operator
        self.dest_operator_id = operator.id
        # Update logical dataflow graph
        # print("Adding new dataflow edge ({},{}) --> ({},{})".format(self.src_operator_id, self.env.operators[self.src_operator_id].name,
        #                                                             self.dest_operator_id, self.env.operators[self.dest_operator_id].name))
        self.env._add_edge(self.src_operator_id,self.dest_operator_id)
        # Keep track of the partitioning strategy and the destination operator
        src_operator = self.env.operators[self.src_operator_id]
        if self.is_partitioned == True:
            s,d = src_operator._get_partition_strategy(self.id)
            src_operator._set_partition_strategy(generate_uuid(), s, operator.id)
        elif src_operator.type == OpType.KeyBy: # Set the output partitioning strategy to shuffle by key
            s = PScheme(PStrategy.ShuffleByKey)
            src_operator._set_partition_strategy(generate_uuid(), s, operator.id)
        else: # No partitioning strategy has been defined - set default
            s = PScheme(PStrategy.Forward)
            src_operator._set_partition_strategy(generate_uuid(), s, operator.id)
        return self.__expand()

    # Sets the level of parallelism for an operator, i.e. its total number of instances
    # Each operator instance corresponds to an actor in the physical dataflow
    def set_parallelism(self, n):
        assert(n>0)
        self.env._set_parallelism(self.src_operator_id,n)
        return self

    ###   Stream Partitioning Strategies   ###
    ### TODO (john): Currently, only forward (default), shuffle, and broadcast are supported

    # Hash-based record shuffling
    def shuffle(self):
        return self.__partition(PStrategy.Shuffle)

    # Broadcasts each record to all downstream instances
    def broadcast(self):
        return self.__partition(PStrategy.Broadcast)

    # Rescales load to downstream instances
    def rescale(self):
        return self.__partition(PStrategy.Rescale)

    # Round-robin partitioning
    def round_robin(self):
        return self.__partition(PStrategy.RoundRobin)

    # User-defined partitioning
    def partition(self,partition_fn):
        return self.__partition(PStrategy.Custom,partition_fn)

    ###   Data Trasnformations   ###
    # TODO (john): Expand set of supported operators.
    # TODO (john): To support event-time windows we need a mechanism for generating and processing watermarks

    # Registers flatmap operator to the environment
    def flat_map(self, flatmap_fn):
        op = Operator(generate_uuid(),OpType.FlatMap,"FlatMap",flatmap_fn,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers keyBy operator to the environment
    def key_by(self, attribute):
        op = Operator(generate_uuid(),OpType.KeyBy,"KeyBy",other=attribute,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers Sum operator to the environment
    def sum(self, attribute):
        op_logic = lambda v_1,v_2: v_1 + v_2
        op = Operator(generate_uuid(),OpType.Sum,"Sum",logic=op_logic,other=attribute,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers window operator to the environment. This is a system time window
    def time_window(self, width):
        op = Operator(generate_uuid(),OpType.TimeWindow,"TimeWindow",num_instances=self.env.config.parallelism,other=width)
        return self.__register(op)

    # Registers filter operator to the environment
    def filter(self, filter_fn):
        op = Operator(generate_uuid(),OpType.Filter,"Filter",filter_fn,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers window join operator to the environment
    def window_join(self, other_stream, join_attribute, window_width):
        op = Operator(generate_uuid(),OpType.WindowJoin,"WindowJoin",flatmap_fn,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers inspect operator to the environment
    def inspect(self,logic):
        op = Operator(generate_uuid(),OpType.Inspect,"Inspect",logic=logic,num_instances=self.env.config.parallelism)
        return self.__register(op)

    # Registers sink operator to the environment
    # TODO (john): A sink now just drops records but it should be able to export data to other systems
    def sink(self):
        op = Operator(generate_uuid(),OpType.Sink,"Sink",num_instances=self.env.config.parallelism)
        return self.__register(op)
