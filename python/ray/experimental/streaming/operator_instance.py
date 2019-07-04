from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
import msgpack
import sys
import time
import types

import ray
import ray.cloudpickle as pickle
import ray.experimental.signal as signal

from ray.experimental import named_actors
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Watermark

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


# Used for attribute selection in Reduce
def _identity(element):
    return element

# Signal denoting that a streaming actor finished processing
# and returned after all its input channels have been closed
class ActorExit(signal.Signal):
    def __init__(self, value=None):
        self.value = value

# Signal denoting that a streaming data source started emitting records
class ActorStart(signal.Signal):
    def __init__(self, value=None):
        self.value = value

class OperatorInstance(object):
    """A streaming operator instance.

    Attributes:
        instance_id (str(UUID)): The id of the instance.
        operator_metadata (Operator): The operator metadata (see: operator.py)
        input (DataInput): The input gate that manages input channels of
        the instance (see: DataInput in communication.py).
        output (DataOutput): The output gate that manages output channels of
        the instance (see: DataOutput in communication.py).
        checkpoint_dir (str): The checkpoint directory
    """

    def __init__(self,
                 instance_id,
                 operator_metadata,
                 input_gate,
                 output_gate,
                 checkpoint_dir):
        self.instance_id = instance_id  # (Operator id, local instance id)
        self.metadata = operator_metadata
        self.input = input_gate
        self.output = output_gate
        self.this_actor = None      # An actor handle to self

        self.key_index = None       # Index for key selection
        self.key_attribute = None   # Attribute name for key selection

        self.num_records_seen = 0   # Number of records seen by the instance

        # Logging-related attributes
        self.logging = self.metadata.logging
        if self.logging:  # Log rates
            self.input.enable_logging()
            self.output.enable_logging()

    # Used for index-based key extraction, e.g. for tuples
    def _index_based_selector(self, record):
        return record[self.key_index]

    # Used for attribute-based key extraction, e.g. for classes
    def _attribute_based_selector(self, record):
        return record[self.key_attribute]

    # Registers own handle
    def _register_handle(self, actor_handle):
        self.this_actor = actor_handle

    # Registers the handle of a destination actor to a channel
    def _register_destination_handle(self, actor_handle, channel_id):
        for channel in self.output.forward_channels:
            if channel.id == channel_id:
                channel.register_destination_actor(actor_handle)
                return
        for channels in self.output.shuffle_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.shuffle_key_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.round_robin_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return
        for channels in self.output.custom_partitioning_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel.register_destination_actor(actor_handle)
                    return

    # Used by apply()
    def _apply(self, record):
        raise Exception("OperatorInstances must implement _apply()")

    # Used by apply_batch()
    def _apply_batch(self, batch):
        raise Exception("OperatorInstances must implement _apply_batch()")

    # Applies the per-record operator logic to a batch of records
    def apply(self, batch, channel_id):
        """ Applies the per-record operator logic to a batch of records.

        Attributes:
            batch (list): The batch of input records
            channel_id (str(UUID)): The id of the input 'channel' the batch
            comes from.
        """
        for record in batch:
            if record is None:
                if self.input._close_channel(channel_id):
                    # logger.debug("Closing channel {}".format(
                    #                                   channel_id))
                    self.output._flush(close=True)
                    signal.send(ActorExit(self.instance_id))
            else:  # Apply the operator-specific logic. This may or may
                # not push a record to the downstream actors.
                self._apply(record)

    # Applies the user-defined operator logic to a batch of records
    def apply_batch(self, batch, channel_id):
        """ Applies the user-defined operator logic to a batch of records.

        Attributes:
            batch (list): The batch of input records
            channel_id (str(UUID)): The id of the input 'channel' the batch
            comes from.
        """
        self._apply_batch(batch)

    # Returns state of stateful operator instances
    def state(self):
        message = "Called state() on an actor of type {}, which is not "
        message += " stateful."
        raise Exception(message)

    # Returns the logged rates (if any)
    def logs(self):
        return (self.instance_id, self.input.rates, self.output.rates)

# A monitoring actor used to keep track of the execution's progress
@ray.remote
class ProgressMonitor(object):
    """An actor used to track the progress of the dataflow execution.

    Attributes:
        running_actors list(actor handles): A list of handles to all actors
        executing the physical dataflow.
    """

    def __init__(self,running_actors):
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

# Map actor
@ray.remote
class Map(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate,
                                  checkpoint_dir)
        # The user-defined map function
        self.map_fn = operator_metadata.logic

    # Applies map logic on a batch of records
    def _apply_batch(self, batch):
        self.output._push_batch(self.map_fn(batch))


# Flatmap actor
@ray.remote
class FlatMap(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces zero or more output records for each record in
    the input stream.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate,
                                  checkpoint_dir)
        # The user-defined flatmap function
        self.flatmap_fn = operator_metadata.logic

    # Applies flatmap logic on a batch of records
    def _apply(self, batch):
        self.output._push_all(self.flatmap_fn(batch))

# Union actor
@ray.remote
class Union(OperatorInstance):
    """A union operator instance that concatenates two or more streams."""

    # Merges all input records into a single output
    def _apply(self, batch):
        self.output._push_batch(batch)

# Join actor
@ray.remote
class Join(OperatorInstance):
    """A join operator instance that joins two streams based on a user-defined
    logic.
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate,
                                  checkpoint_dir)
        # The user-defined join logic
        self.join_logic = operator_metadata.logic
        assert(self.join_logic is not None), (self.join_logic)
        self.left = operator_metadata.left_input_operator_id
        self.right = operator_metadata.right_input_operator_id
        self.process_logic = None

    # Applies join logic on a batch of records
    def apply(self, batches, channel_id, source_operator_id, checkpoint_epoch):
        # Distringuish between left and right input and set the right logic
        # We expect this to be as cheap as setting a pointer
        if source_operator_id == self.left:
            self.process_logic = self.join_logic.process_left
        else:
            assert source_operator_id == self.right
            self.process_logic = self.join_logic.process_right
        super().apply(batches, channel_id, source_operator_id, checkpoint_epoch)

    def _apply(self, record):
        if record["event_type"] == "Watermark":  # It is a watermark
            self.output._push(record)
        else:
            self.output._push_all(self.process_logic(record))


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
                 output_gate, checkpoint_dir, config=None):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate,
                                  output_gate, checkpoint_dir, config)
        self.state = {}                             # key -> value
        self.reduce_fn = operator_metadata.logic    # Reduce function
        # Set the attribute selector
        self.attribute_selector = operator_metadata.attribute_selector
        if self.attribute_selector is None:
            self.attribute_selector = _identity
        elif isinstance(self.attribute_selector, int):
            self.key_index = self.attribute_selector
            self.attribute_selector = self._index_based_selector
        elif isinstance(self.attribute_selector, str):
            self.key_attribute = self.attribute_selector
            self.attribute_selector = self._attribute_based_selector
        elif not isinstance(self.attribute_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # Task-based reduce execution on a set of batches
    def _apply(self, record):
        _time, key, rest = record
        new_value = self.attribute_selector(record)
        # TODO (john): Is there a way to update state with
        # a single dictionary lookup?
        try:
            old_value = self.state[key]
            new_value = self.reduce_fn(old_value, new_value)
            self.state[key] = new_value
        except KeyError:  # Key does not exist in state
            self.state.setdefault(key, new_value)
        self.output._push((_time, key, new_value))

    # Returns the local state of the actor
    def state(self):
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
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id,
                                  operator_metadata, input_gate, output_gate,
                                  checkpoint_dir)
        # Set the key selector
        self.key_selector = operator_metadata.key_selector
        if isinstance(self.key_selector, int):
            self.key_index = self.key_selector
            self.key_selector = self._index_based_selector
        elif isinstance(self.key_selector, str):
            self.key_attribute = self.key_selector
            self.key_selector = self._attribute_based_selector
        elif not isinstance(self.key_selector, types.FunctionType):
            sys.exit("Unrecognized or unsupported key selector.")

    # Task-based keyby execution on a set of batches
    def _apply(self, record):
        key = self.key_selector(record)
        self.output._push((key,record))

# A custom source actor
@ray.remote
class Source(OperatorInstance):
    """A source emitting records.

    A user-defined source must implement the following methods:
    - init()
    - get_next()
    - get_next_batch()
    - close()
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate,
                                  checkpoint_dir)

        _, local_instance_id = instance_id
        source_objects = operator_metadata.sources
        # Distinguish between single- and multi-instance sources
        self.source = source_objects[local_instance_id] if isinstance(
                            source_objects, list) else source_objects
        self.source.init()  # Initialize the source
        self.watermark_interval = operator_metadata.watermark_interval
        self.max_event_time = 0  # Max event timestamp seen so far


    def __watermark(self, record_batch):
        """Generates one or more watermarks.

        This method checks if a watermark must be emitted by the source based
        on the event times seen so far and a user-defined watermark interval.

        Attributes:
             record_batch (list): The list of input records,
             each one with an associated event time.
        """
        max_timestamp = 0
        for record in record_batch:
            try:
                event_time = record["dateTime"]
            except AttributeError as e:
                raise Exception(e)
            max_timestamp = max(event_time, self.max_event_time)
        if (max_timestamp >=
                self.max_event_time + self.watermark_interval):
            # Emit watermark
            log_message = "Source emitting watermark {} due to {}"
            log_message += "on interval {}"
            logger.debug(log_message.format(self.max_event_time,
                                            max_timestamp,
                                            self.watermark_interval))
            # Use obj.__dict__ instead of the object itself
            self.output._push(Watermark(self.max_event_time,
                                        time.time()).__dict__)
            # Update max event time seen so far
            self.max_event_time = max_timestamp

    # Starts the source
    def start(self):
        signal.send(ActorStart(self.instance_id))
        self.start_time = time.time()
        batch_size = operator_metadata.batch_size
        while True:
            record_batch = self.source.get_next_batch(batch_size)
            if record_batch is None:  # Source is exhausted
                logger.info("Source throuhgput: {}".format(
                            self.num_records_seen / (
                            time.time() - self.start_time)))
                self.output._flush(close=True)  # Flush and close output
                signal.send(ActorExit(self.instance_id))  # Send exit signal
                try:
                    self.source.close()
                except AttributeError as e:
                    raise Exception(e)
                return
            self.output._push_batch(record_batch, -1)
            # TODO (john): Handle the case a record does not have event time
            # logger.debug(
            #   "Source watermark: {}. Last record time: {}".format(
            #         self.max_event_time, record_batch[-1]["dateTime"]))
            if self.watermark_interval > 0:  # Watermarks are activated
                self.__watermark(record_batch)
            # Measure source throughput every 10K records
            self.num_records_seen += len(record_batch)
            if self.num_records_seen % 10000 == 0:
                logger.info("Source throughput: {}".format(
                    self.num_records_seen / (time.time() - self.start_time)))

# A custom sink actor
@ray.remote
class Sink(OperatorInstance):
    """A dataflow sink.

    A user-defined source must implement the following methods:
    - init()
    - evict()
    - evict_batch()
    - close()
    """

    def __init__(self, instance_id, operator_metadata, input_gate,
                 output_gate, checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate,
                                  checkpoint_dir)
        # The user-defined sink
        self.sink = operator_metadata.sink
        self.sink.init()  # Initialize the sink

    # Task-based sink execution on a record
    def _apply(self, record):
        # logger.debug("SINK %s", record)
        self.sink.evict(record)

    # Task-based sink execution on a batch of records
    def _apply_batch(self, batch):
        # logger.debug("SINK %s", record)
        self.sink.evict_batch(batch)

    # Returns the local state of the sink instance
    def state(self):
        try:  # There might be no 'get_state()' method implemented
            return (self.instance_id, self.sink.get_state())
        except AttributeError as e:
            raise Exception(e)
