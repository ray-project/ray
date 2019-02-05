import threading
import ray

from ray.slib.communication import  *

###
###   Each Ray actor corresponds to an operator instance in the physical dataflow
###   Actors communicate using batched queues as data channels (no standing TCP connections)
###   Currently, batched queues are based on Eric's implementation (see: batched_queue.py)
###

# TODO (john): Source actor
@ray.remote
class Source(object):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        self.instance_id = instance_id
        self.filepath = operator_metadata.other[0]
        self.input = input_gate
        self.output = output_gate

    # Starts the source
    def start(self):
        while True:
            pass

# TODO (john): This should also be an actor
# Reads text file line by line
class ReadTextFile(object):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        self.instance_id = instance_id
        self.filepath = operator_metadata.other_args
        self.input = input_gate
        self.output = output_gate
        self.reader = open(self.filepath,"r")   # TODO (john): Handle possible exception here

    # Read input file line by line
    def read(self):
        while True:
            record = self.reader.readline()
            if not record:  # Flush any remaining records to the object store and close the file
                self.output.push(record)    # Propagate 'None'
                for c in self.output.output_channels:
                    c.queue._flush_writes()
                self.reader.close()
                break
            self.output.push(record[:-1])

# Flatmap actor
@ray.remote
class FlatMap(object):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        self.instance_id = instance_id
        self.flatmap_fn = operator_metadata.logic
        self.input = input_gate
        self.output = output_gate

    # Applies the splitter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def flatmap(self):
        while True:
            record = self.input.pull();
            self.output.push_all(self.flatmap_fn(record))
            if not record:
                break

# Filter actor
@ray.remote
class Filter(object):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        self.instance_id = instance_id
        self.filter_fn = operator_metadata.logic
        self.input = input_gate
        self.output = output_gate

    # Applies the filter to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def filter(self):
        while True:
            record = self.input.pull();
            if not record:  # Propagate 'None' and return
                self.output.push(res)
                break
            res = self.filter_fn(record)
            if res == True: self.output.push(res)

# Inspect actor
@ray.remote
class Inspect(object):
    def __init__(self, instance_id, operator_metadata, input_gate, output_gate):
        self.instance_id = instance_id
        self.inspect_fn = operator_metadata.logic
        self.input = input_gate
        self.output = output_gate

    # Applies the inspect logic to the records of the input stream(s)
    # and pushes resulting records to the output stream(s)
    def inspect(self):
        while True:
            record = self.input.pull();
            self.inspect_fn(record)
            self.output.push(record)
            if not record: break

# TODO(john): Time window actor (uses system time)
@ray.remote
class TimeWindow(object):
    def __init__(self, queue, width):
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
