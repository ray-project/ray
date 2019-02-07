import sys
import threading
import time
import ray

from ray.slib.communication import  *

NORMAL_EXIT_STATUS = 0

###
###   Each Ray actor corresponds to an operator instance in the physical dataflow
###   Actors communicate using batched queues as data channels (no standing TCP connections)
###   Currently, batched queues are based on Eric's implementation (see: batched_queue.py)
###

class Actor(object):
    def __init__(self, instance_id, input_gate, output_gate):
        self.instance_id = instance_id
        self.input = input_gate
        self.output = output_gate
        # Enable writes
        for c in self.output.fb_channels:
            c.queue.enable_writes()
        for cs in self.output.shuffle_channels:
            for c in cs: c.queue.enable_writes()
        # TODO (john): Add more channel types here

    # Starts the actor
    def start(self):
        pass

# TODO (john): Source actor
@ray.remote
class Source(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
        self.filepath = operator_metadata.other[0]

    # Starts the source
    def start(self):
        while True:
            pass

# TODO (john): This should also be an actor
# Reads text file line by line
class ReadTextFile(Actor):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        super().__init__(instance_id, input_gate, output_gate)
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
            record = self.input._pull();
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
            record = self.input._pull();
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

    # Applies the inspect logic to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def start(self):
        while True:
            record = self.input._pull();
            if record == None:
                self.output._flush(close=True)
                return NORMAL_EXIT_STATUS
            self.output._push(record)
            self.inspect_fn(record)

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
