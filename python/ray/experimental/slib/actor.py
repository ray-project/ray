import sys
import threading
import time
import ray

from ray.experimental.slib.communication import  *

NORMAL_EXIT_STATUS = 0

###
###   Each Ray actor corresponds to an operator instance in the physical dataflow
###   Actors communicate using batched queues as data channels (no standing TCP connections)
###   Currently, batched queues are based on Eric's implementation (see: batched_queue.py)
###

# TODO (john): Specify the interface of state keepers
class Actor(object):
    def __init__(self, instance_id, input_gate, output_gate, state_keepers=None):
        self.instance_id = instance_id
        self.input = input_gate
        self.output = output_gate
        self.state_keepers = state_keepers      # Handle(s) to one or more user-defined actors
                                                # that can retrieve actor's state
        # Enable writes
        for c in self.output.fb_channels:
            c.queue.enable_writes()
        for cs in self.output.shuffle_channels:
            for c in cs:
                c.queue.enable_writes()
        for cs in self.output.shuffle_key_channels:
            for c in cs: c.queue.enable_writes()
        # TODO (john): Add more channel types here

    # Starts the actor
    def start(self):
        pass

# TODO (john): Source actor
@ray.remote
class Source(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate, state_keepers=None):
        super().__init__(instance_id, input_gate, output_gate, state_keepers)
        self.filepath = operator_metadata.other[0]

    # Starts the source
    def start(self):
        while True:
            pass

# TODO (john): This should also be an actor
# Reads text file line by line
class ReadTextFile(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate, state_keepers=None):
        super().__init__(instance_id, input_gate, output_gate, state_keepers)
        self.filepath = operator_metadata.other_args
        self.reader = open(self.filepath,"r")   # TODO (john): Handle possible exception here

    # Read input file line by line
    def start(self):
        while True:
            record = self.reader.readline()
            if not record:  # Flush any remaining records to plasma and close the file
                self.output._flush(close=True)
                self.reader.close()
                return NORMAL_EXIT_STATUS
            self.output._push(record[:-1])  # Push after removing newline characters

# Map actor
@ray.remote
class Map(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.map_fn = operator_metadata.logic

    # Applies the mapper each record of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            self.output._push(self.map_fn(record))

# Flatmap actor
@ray.remote
class FlatMap(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.flatmap_fn = operator_metadata.logic

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            self.output._push_all(self.flatmap_fn(record))

# Filter actor
@ray.remote
class Filter(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.filter_fn = operator_metadata.logic

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:  # Close channel and return
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            if self.filter_fn(record): self.output._push(record)

# Inspect actor
@ray.remote
class Inspect(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.inspect_fn = operator_metadata.logic

    # Applies the inspect logic (e.g. print) to the records of the input stream(s)
    # and leaves stream unaffected by simply pushing the records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            self.output._push(record)
            self.inspect_fn(record)

# Reduce actor
@ray.remote
class Reduce(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.reduce_fn = operator_metadata.logic
        self.value_attribute = operator_metadata.other_args
        self.state = {}     # key -> value

    # Combines the input value for a key with the last reduced value for that key
    # to produce a new value and output the result as (key,new value)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:
                self.output._flush(close=True)
                del self.state
                return NORMAL_EXIT_STATUS
            # TODO (john): General key extraction from objects
            k,_ = record
            new_value = record[self.value_attribute]
            try: # TODO (john): Is there a way to update state with a single dictionary lookup?
                old_value = self.state[k]
                new_value = self.reduce_fn(old_value,new_value)
                self.state[k] = new_value
            except: # Key does not exist in state
                self.state.setdefault(k,new_value)
            self.output._push((k,new_value))

@ray.remote
class KeyBy(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.key_attribute = operator_metadata.other_args

    # Maps each input record to a tuple of the form (key,record) and shuffles output by hash(key)
    def start(self):
        while True:
            record = self.input._pull()
            if record == None:
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            # TODO (john): General key extraction from objects
            k,v = record
            self.output._push((k,v))

# TODO(john): Time window actor (uses system time)
@ray.remote
class TimeWindow(Actor):
    def __init__(self, queue, width):
        super().__init__(instance_id, input_gate, output_gate)
        self.queue = queue
        self.width = width     # In milliseconds

    def time_window(self):
        # Read next batch from queue
        # profiling
        while True:
            #with ray.profile("...")
            #    # something
                pass
        # apply function
        # return a DataStream
