from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys
import time
import types

import ray
import ray.experimental.signal as signal

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

#
# Each Ray actor corresponds to an operator instance in the physical dataflow
# Actors communicate using batched queues as data channels (no standing TCP
# connections)
# Currently, batched queues are based on Eric's implementation (see:
# batched_queue.py)


def _identity(element):
    return element


# Signal denoting that a streaming actor finished processing
# and returned after all its input channels have been closed
class ActorExit(signal.Signal):
    def __init__(self, value=None):
        self.value = value


# Signal denoting that a streaming data source has started emitting records
class ActorStart(signal.Signal):
    def __init__(self, value=None):
        self.value = value


# A monitoring actor used to keep track of the execution's progress
@ray.remote
class ProgressMonitor(object):
    """An actor used to track the progress of the dataflow execution.

    Attributes:
        running_actors list(actor handles): A list of handles to all actors
        executing the physical dataflow.
    """

    def __init__(self, running_actors):
        self.running_actors = running_actors  # Actor handles
        self.start_signals = []
        self.exit_signals = []
        logger.debug("Running actors: {}".format(self.running_actors))

    # Returns when the dataflow execution is over
    def all_exit_signals(self):
        while True:
            # Block until the ActorExit signal has been
            # received from each actor executing the dataflow
            signals = signal.receive(self.running_actors)
            for _, received_signal in signals:
                if isinstance(received_signal, ActorExit):
                    self.exit_signals.append(received_signal)
            if len(self.exit_signals) == len(self.running_actors):
                return

    # Returns after receiving all ActorStart signals for a list of actors
    def start_signals(self, actor_handles):
        while True:
            # Block until the ActorStart signal has been
            # received from each actor in the given list
            signals = signal.receive(actor_handles)
            for _, received_signal in signals:
                if isinstance(received_signal, ActorStart):
                    self.start_signals.append(received_signal)
            if len(self.start_signals) == len(actor_handles):
                return


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

    def __init__(self, instance_id, input_gate, output_gate,
                 state_keeper=None):
        self.key_index = None  # Index for key selection
        self.key_attribute = None  # Attribute name for key selection
        self.instance_id = instance_id
        self.input = input_gate
        self.output = output_gate
        # Handle(s) to one or more user-defined actors
        # that can retrieve actor's state
        self.state_keeper = state_keeper
        # Enable writes
        for channel in self.output.forward_channels:
            channel.queue.enable_writes()
        for channels in self.output.shuffle_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.round_robin_channels:
            for channel in channels:
                channel.queue.enable_writes()
        # TODO (john): Add more channel types here

    # Used for index-based key extraction, e.g. for tuples
    def index_based_selector(self, record):
        return record[self.key_index]

    # Used for attribute-based key extraction, e.g. for classes
    def attribute_based_selector(self, record):
        return vars(record)[self.key_attribute]

    # Registers actor's handle so that the actor can schedule itself
    def _register_handle(self, actor_handle):
        self.this_actor = actor_handle

    # Registers the handle of a destination actor to an output channel
    def _register_destination_handle(self, actor_handle, channel_id):
        for channel in self.output.forward_channels:
            if channel.id == channel_id:
                channel._register_destination_actor(actor_handle)
                return
        for channels in self.output.shuffle_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel._register_destination_actor(actor_handle)
                    return
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel._register_destination_actor(actor_handle)
                    return
        for channels in self.output.round_robin_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel._register_destination_actor(actor_handle)
                    return
        # TODO (john): Add more channel types here

    # Starts the actor
    def start(self):
        pass


# A source actor that reads a text file line by line
@ray.remote
class ReadTextFile(OperatorInstance):
    """A source operator instance that reads a text file line by line.

    Attributes:
        filepath (string): The path to the input file.
    """

    def __init__(self,
                 instance_id,
                 operator_metadata,
                 input_gate,
                 output_gate,
                 state_keepers=None):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate,
                                  state_keepers)
        self.filepath = operator_metadata.other_args
        # TODO (john): Handle possible exception here
        self.reader = open(self.filepath, "r")

    # Read input file line by line
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF
            if not record:
                # Flush any remaining records to plasma and close the file
                self.output._flush(close=True)
                self.reader.close()
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(
                record[:-1])  # Push after removing newline characters


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

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        self.map_fn = operator_metadata.logic

    # Applies the mapper each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        start = time.time()
        elements = 0
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                logger.debug("[map {}] read/writes per second: {}".format(
                    self.instance_id, elements / (time.time() - start)))
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(self.map_fn(record))
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

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        self.flatmap_fn = operator_metadata.logic

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push_all(self.flatmap_fn(record))


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

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        self.filter_fn = operator_metadata.logic

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:  # Close channel and return
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            if self.filter_fn(record):
                self.output._push(record)


# Inspect actor
@ray.remote
class Inspect(OperatorInstance):
    """A inspect operator instance that inspects the content of the stream.

    Inspect is useful for printing the records in the stream.

    Attributes:
         inspect_fn (function): The user-defined inspect logic.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        self.inspect_fn = operator_metadata.logic

    # Applies the inspect logic (e.g. print) to the records of
    # the input stream(s)
    # and leaves stream unaffected by simply pushing the records to
    # the output stream(s)
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(record)
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

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate,
                                  operator_metadata.state_actor)
        self.reduce_fn = operator_metadata.logic
        # Set the attribute selector
        self.attribute_selector = operator_metadata.other_args
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
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                del self.state
                signal.send(ActorExit(self.instance_id))
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
            self.output._push((key, new_value))

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

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        # Set the key selector
        self.key_selector = operator_metadata.other_args
        if isinstance(self.key_selector, int):
            self.key_index = self.key_selector
            self.key_selector = self.index_based_selector
        elif isinstance(self.key_selector, str):
            self.key_attribute = self.key_selector
            self.key_selector = self.attribute_based_selector
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # The actual partitioning is done by the output gate
    def start(self):
        signal.send(ActorStart(self.instance_id))
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                signal.send(ActorExit(self.instance_id))
                return
            key = self.key_selector(record)
            self.output._push((key, record))


# A custom source actor
@ray.remote
class Source(OperatorInstance):
    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate):
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        # The user-defined source with a get_next() method
        self.source = operator_metadata.other_args

    # Starts the source by calling get_next() repeatedly
    def start(self):
        signal.send(ActorStart(self.instance_id))
        start = time.time()
        elements = 0
        while True:
            next = self.source.get_next()
            if next is None:
                self.output._flush(close=True)
                logger.debug("[writer {}] puts per second: {}".format(
                    self.instance_id, elements / (time.time() - start)))
                signal.send(ActorExit(self.instance_id))
                return
            self.output._push(next)
            elements += 1


# TODO(john): Time window actor (uses system time)
@ray.remote
class TimeWindow(OperatorInstance):
    def __init__(self, queue, width):
        self.width = width  # In milliseconds

    def time_window(self):
        while True:
            pass
