from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

#
# Each Ray actor corresponds to an operator instance in the physical dataflow
# Actors communicate using batched queues as data channels (no standing TCP
# connections)
# Currently, batched queues are based on Eric's implementation (see:
# batched_queue.py)


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

    def __init__(self,
                 instance_id,
                 input_gate,
                 output_gate,
                 state_keepers=None):
        self.instance_id = instance_id
        self.input = input_gate
        self.output = output_gate
        # Handle(s) to one or more user-defined actors
        # that can retrieve actor's state
        self.state_keepers = state_keepers
        # Enable writes
        for channel in self.output.forward_channels:
            channel.queue.enable_writes()
        for channels in self.output.shuffle_channels:
            for channel in channels:
                channel.queue.enable_writes()
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                channel.queue.enable_writes()
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
        while True:
            record = self.reader.readline()
            # Reader returns empty string ('') on EOF
            if not record:
                # Flush any remaining records to plasma and close the file
                self.output._flush(close=True)
                self.reader.close()
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
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                return
            self.output._push(self.map_fn(record))


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
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
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
        while True:
            record = self.input._pull()
            if record is None:  # Close channel and return
                self.output._flush(close=True)
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
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
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
        OperatorInstance.__init__(self, instance_id, input_gate, output_gate)
        self.reduce_fn = operator_metadata.logic
        self.value_attribute = operator_metadata.other_args
        self.state = {}  # key -> value

    # Combines the input value for a key with the last reduced
    # value for that key to produce a new value and output the result
    # as (key,new value)
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                del self.state
                return
            # TODO (john): General key extraction from objects
            k, _ = record
            new_value = record[self.value_attribute]
            # TODO (john): Is there a way to update state with
            # a single dictionary lookup?
            try:
                old_value = self.state[k]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[k] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(k, new_value)
            self.output._push((k, new_value))


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
        self.key_attribute = operator_metadata.other_args

    # Maps each input record to a tuple of the form (key,record)
    # and shuffles output by hash(key)
    def start(self):
        while True:
            record = self.input._pull()
            if record is None:
                self.output._flush(close=True)
                return
            # TODO (john): General key extraction from objects
            k, v = record
            self.output._push((k, v))


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
        while True:
            next = self.source.get_next()
            if next is None:
                self.output._flush(close=True)
                return
            self.output._push(next)


# TODO(john): Time window actor (uses system time)
@ray.remote
class TimeWindow(OperatorInstance):
    def __init__(self, queue, width):
        self.width = width  # In milliseconds

    def time_window(self):
        while True:
            pass
