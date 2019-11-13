from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys
import time
import types
import threading

import ray
from ray.function_manager import FunctionDescriptor
from ray.streaming.communication import DataInput
from ray.streaming.communication import DataOutput
from ray.streaming.config import Config
from ray.streaming.queue.memory_queue import MemQueueLinkImpl
import ray.streaming.queue.streaming_queue as streaming_queue

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


def _identity(element):
    return element


# TODO (john): Specify the interface of state keepers
class OperatorInstance(object):
    """A streaming operator instance.

    Attributes:
        instance_id (UUID): The id of the instance.
        input (DataInput): The input gate that manages input channels of
        the instance (see: DataInput in communication.py).
        input (DataOutput): The output gate that manages output channels of
        the instance (see: DataOutput in communication.py).
        state_keepers (list): A list of actor handlers to query the state of
        the operator instance.
    """

    def __init__(self, operator, instance_id, input_channels, output_channels,
                 state_keeper=None):
        self.env = None
        self.operator = operator
        self.key_index = None  # Index for key selection
        self.key_attribute = None  # Attribute name for key selection
        self.instance_id = instance_id
        self.queue_link = None
        self.input_channels = input_channels
        self.output_channels = output_channels
        self.input_gate = None
        self.output_gate = None
        # Handle(s) to one or more user-defined actors
        # that can retrieve actor's state
        self.state_keeper = state_keeper

    # Registers actor's handle so that the actor can schedule itself
    def register_handle(self, actor_handle):
        self.this_actor = actor_handle

    # Used for index-based key extraction, e.g. for tuples
    def index_based_selector(self, record):
        return record[self.key_index]

    # Used for attribute-based key extraction, e.g. for classes
    def attribute_based_selector(self, record):
        return vars(record)[self.key_attribute]

    def init(self, env):
        """init streaming actor"""
        logger.info("init operator instance %s", self.__class__.__name__)
        self.env = env
        if self.env.config.queue_type == Config.MEMORY_QUEUE:
            self.queue_link = MemQueueLinkImpl()
        else:
            sync_func = FunctionDescriptor(__name__, self.on_streaming_transfer_sync.__name__,
                                           self.__class__.__name__)
            async_func = FunctionDescriptor(__name__, self.on_streaming_transfer.__name__,
                                            self.__class__.__name__)
            self.queue_link = streaming_queue.QueueLinkImpl(sync_func, async_func)
        runtime_conf = {Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context().current_driver_id}
        self.queue_link.set_ray_runtime(runtime_conf)
        if len(self.input_channels) > 0:
            self.input_gate = DataInput(env, self.queue_link, self.input_channels)
            self.input_gate.init()
        if len(self.output_channels) > 0:
            self.output_gate = DataOutput(env, self.queue_link, self.output_channels,
                                          self.operator.partitioning_strategies)
            self.output_gate.init()
        return True

    # Starts the actor
    def start(self):
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()
        self.t.join()

    # run
    def run(self):
        pass

    def close(self):
        if self.input_gate:
            self.input_gate.close()
        if self.output_gate:
            self.output_gate.close()

    def on_streaming_transfer(self, buffer: bytes):
        """used in direct call mode"""
        self.queue_link.on_streaming_transfer(buffer)

    def on_streaming_transfer_sync(self, buffer: bytes):
        """used in direct call mode"""
        self.queue_link.on_streaming_transfer_sync(buffer)


# A source actor that reads a text file line by line
@ray.remote
class ReadTextFile(OperatorInstance):
    """A source operator instance that reads a text file line by line.

    Attributes:
        filepath (string): The path to the input file.
    """

    def __init__(self,
                 instance_id,
                 operator,
                 input_gate,
                 output_gate,
                 state_keepers=None):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate,
                                  state_keepers)
        self.filepath = operator.other_args
        # TODO (john): Handle possible exception here
        self.reader = open(self.filepath, "r")

    # Read input file line by line
    def run(self):
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF
            if not record:
                self.close()
                self.reader.close()
                return
            self.output_gate.push(record[:-1])  # Push after removing newline characters


# Map actor
@ray.remote
class Map(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.

    Attributes:
        map_fn (function): The user-defined function.
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        self.map_fn = operator.logic

    # Applies the mapper each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self):
        start = time.time()
        elements = 0
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            self.output_gate.push(self.map_fn(record))
            elements += 1


# Flatmap actor
@ray.remote
class FlatMap(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces one or more output records for each record in
    the input stream.

    Attributes:
        flatmap_fn (function): The user-defined function.
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        self.flatmap_fn = operator.logic

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self):
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            self.output_gate.push_all(self.flatmap_fn(record))


# Filter actor
@ray.remote
class Filter(OperatorInstance):
    """A filter operator instance that applies a user-defined filter to
    each record of the stream.

    Output records are those that pass the filter, i.e. those for which
    the filter function returns True.

    Attributes:
        filter_fn (function): The user-defined boolean function.
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        self.filter_fn = operator.logic

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def run(self):
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            if self.filter_fn(record):
                self.output_gate.push(record)


# Inspect actor
@ray.remote
class Inspect(OperatorInstance):
    """A inspect operator instance that inspects the content of the stream.

    Inspect is useful for printing the records in the stream.

    Attributes:
         inspect_fn (function): The user-defined inspect logic.
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        self.inspect_fn = operator.logic

    def run(self):
        # Applies the inspect logic (e.g. print) to the records of
        # the input stream(s)
        # and leaves stream unaffected by simply pushing the records to
        # the output stream(s)
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            if self.output_gate:
                self.output_gate.push(record)
            self.inspect_fn(record)


# Reduce actor
@ray.remote
class Reduce(OperatorInstance):
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.

    Attributes:
        reduce_fn (function): The user-defined reduce logic.
        value_attribute (int): The index of the value to reduce
        (assuming tuple records).
        state (dict): A mapping from keys to values.
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate,
                                  operator.state_actor)
        self.reduce_fn = operator.logic
        # Set the attribute selector
        self.attribute_selector = operator.other_args
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self.attribute_selector = self.index_based_selector
        elif isinstance(self.attribute_selector, str):
            self.key_attribute = self.attribute_selector
            self.attribute_selector = self.attribute_based_selector
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")
        self.state = {}  # key -> value

    # Combines the input value for a key with the last reduced
    # value for that key to produce a new value.
    # Outputs the result as (key,new value)
    def run(self):
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            key, rest = record
            new_value = self.attribute_selector(rest)
            # TODO (john): Is there a way to update state with
            # a single dictionary lookup?
            try:
                old_value = self.state[key]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[key] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(key, new_value)
            self.output_gate.push((key, new_value))

    # Returns the state of the actor
    def get_state(self):
        return self.state


@ray.remote
class KeyBy(OperatorInstance):
    """A key_by operator instance that physically partitions the
    stream based on a key.

    Attributes:
        key_attribute (int): The index of the value to reduce
        (assuming tuple records).
    """

    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        # Set the key selector
        self.key_selector = operator.other_args
        if isinstance(self.key_selector, int):
            self.key_index = self.key_selector
            self.key_selector = self.index_based_selector
        elif isinstance(self.key_selector, str):
            self.key_attribute = self.key_selector
            self.key_selector = self.attribute_based_selector
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # The actual partitioning is done by the output gate
    def run(self):
        while True:
            record = self.input_gate.pull()
            if record is None:
                self.close()
                return
            key = self.key_selector(record)
            self.output_gate.push((key, record))


# A custom source actor
@ray.remote
class Source(OperatorInstance):
    def __init__(self, instance_id, operator, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, operator, instance_id, input_gate, output_gate)
        # The user-defined source with a get_next() method
        self.source = operator.other_args

    # Starts the source by calling get_next() repeatedly
    def run(self):
        start = time.time()
        elements = 0
        while True:
            record = self.source.get_next()
            if not record:
                logger.debug("[writer {}] puts per second: {}".format(
                    self.instance_id, elements / (time.time() - start)))
                self.close()
                return
            self.output_gate.push(record)
            elements += 1


# TODO(john): Time window actor (uses system time)
@ray.remote
class TimeWindow(OperatorInstance):
    def __init__(self, queue, width):
        self.width = width  # In milliseconds

    def time_window(self):
        while True:
            pass
