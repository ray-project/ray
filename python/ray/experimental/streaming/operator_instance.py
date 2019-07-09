from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
import types

import ray
import ray.experimental.signal as signal

from ray.experimental.streaming.benchmarks.macro.nexmark.event import Watermark

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


# Signal denoting that a streaming actor finished processing
# and returned after all its input channels have been closed
class ActorExit(signal.Signal):
    def __init__(self, value=None):
        self.value = value


# Signal denoting that a streaming data source has started emitting records
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
        checkpoint_dir (str): The checkpoints directory
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        self.instance_id = instance_id  # (Operator id, local instance id)
        self.metadata = operator_metadata
        self.input = input_gate
        self.output = output_gate
        self.this_actor = None  # Owns actor handle

        self.key_index = None  # Index for key selection
        self.key_attribute = None  # Attribute name for key selection

        self.num_records_seen = 0  # Number of records seen by the actor

        # TODO (john): Should be initialized based on the user-defined offset
        self.last_emitted_watermark = 0
        # Epochs per input channel
        self.epochs = [0] * len(self.input.input_channels)

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

    # Registers own actor handle
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
        for channels in self.output.custom_partitioning_channels:
            for channel in channels:
                if channel.id == channel_id:
                    channel._register_destination_actor(actor_handle)
                    return

    # Updates progress information (called by the upstream operator instance)
    def _update_progress(self, watermark, input_channel_id):
        """Updates progress information as received from upstream and
        propagates this information downstream.

        This method checks if a watermark appearing in a channel must be
        forwarded to all output channels (downstream operator instances).
        For a source, the watermark is always forwarded to the output.
        For any other operator, the watermark is not necessarily forwarded;
        in this case, the forwarded watermark is the minimum watermark
        received from all input channels of the operator that is strictly
        larger than the previously forwarded watermark.

        The method assumes single-dimensional watermarks that increase
        monotonically.

        Attributes:
             watermark (dict): The watermark object's dict
             input_channel_id (int): The index of the input channel the
             watermark came from
        """
        assert input_channel_id is not None
        # Set the last seen watermark in this channel
        previous_epoch = self.input.input_channels[input_channel_id]
        current_epoch = watermark["event_time"]
        # Epochs from the same input can only increase monotonically
        assert previous_epoch < current_epoch
        self.epochs[input_channel_id] = current_epoch
        # Find minimum watermark amongst all input channels
        minimum_watermark = min(self.epochs.values())
        if minimum_watermark > self.last_emitted_watermark:  # Forward
            self.last_emitted_watermark = minimum_watermark
            # Update the epoch and propagate the same watermark downstream
            watermark["event_time"] = minimum_watermark
            # logger.info("Emitting watermark: {}".format(
            #             self.last_emitted_watermark))
            self.output._broadcast_watermark(watermark)

    # Closes an input channel (called by the upstream operator instance)
    def _close_input(self, channel_id):
        """ Closes an input channel and exits if all inputs have been
        closed.

        Attributes:
            channel_id (str(UUID)): The id of the input 'channel' to close.
        """
        if self.input._close_channel(channel_id):
            # logger.debug("Closing channel {}".format(channel_id))
            self.output._close()
            signal.send(ActorExit(self.instance_id))

    # This method must be implemented by the subclasses
    def _apply(self, batch, input_channel_id=None):
        """ Applies the user-defined operator logic to a batch of records.

        Attributes:
            batch (list): The batch of input records
            input_channel_id (str(UUID)): The id of the input 'channel' the
            batch comes from.
        """
        raise Exception("OperatorInstances must implement _apply()")

    # This method must be implemented by the subclasses
    def _apply_batch(self, batch, input_channel_id=None):
        """ Applies the user-defined operator logic to a batch of records.

        Attributes:
            batch (list): The batch of input records
            input_channel_id (str(UUID)): The id of the input 'channel' the
            batch comes from.
        """
        raise Exception("OperatorInstances must implement _apply_batch()")

    # Returns state of stateful operator instances
    def state(self):
        message = "Called state() on an instance of type {}, which is not "
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


# Map actor
@ray.remote
class Map(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A map produces exactly one output record for each record in
    the input stream.
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        # The user-defined map function
        self.map_fn = operator_metadata.logic

    # Applies map logic on a batch of records (one record at a time)
    def _apply(self, batch, input_channel_id=None):
        for record in batch:
            self.output._push(self.map_fn(record))

    # Applies map logic on a batch of records
    def _apply_batch(self, batch, input_channel_id=None):
        self.output._push_batch(self.map_fn(batch))


# Flatmap actor
@ray.remote
class FlatMap(OperatorInstance):
    """A map operator instance that applies a user-defined
    stream transformation.

    A flatmap produces zero or more output records for each record in
    the input stream.
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        # The user-defined flatmap function
        self.flatmap_fn = operator_metadata.logic
        self.max_batch_size = input_gate[0].queue_config.max_batch_size

    # TODO (john): Batches generated by the flatmap may exceed max_batch_size
    # and should be pushed to the output gradually, as the records are being
    # produced

    # Applies flatmap logic on a batch of records
    def _apply(self, batch, input_channel_id=None):
        for record in batch:
            self.output._push_batch(self.flatmap_fn(record))

    # Applies flatmap logic on a batch of records
    def _apply_batch(self, batch, input_channel_id=None):
        self.output._push_batch(self.flatmap_fn(batch))


# Union actor
@ray.remote
class Union(OperatorInstance):
    """A union operator instance that concatenates two or more streams.
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        pass

    # Merges all input records into a single output
    def _apply(self, batch, input_channel_id=None):
        self.output._push_batch(batch)


# Used as default attribute selection in the Reduce operator
def _identity(element):
    return element


# Reduce actor
@ray.remote
class Reduce(OperatorInstance):
    """A reduce operator instance that combines a new value for a key
    with the last reduced one according to a user-defined logic.

    Expects an input stream of tuples of the form (key, record).

    Attributes:
        reduce_fn (function): The user-defined reduce logic.
        state (dict): A mapping from keys to values.
    """

    def __init__(self,
                 instance_id,
                 operator_metadata,
                 input_gate,
                 output_gate,
                 checkpoint_dir,
                 config=None):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        self.state = {}  # key -> value
        self.reduce_fn = operator_metadata.logic  # Reduce function
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
            raise Exception("Unrecognized or unsupported key selector.")

    # Applies reduce logic on a batch of records
    def _apply(self, batch, input_channel_id=None):
        for record in batch:
            key, data = record  # Expects a keyed stream
            new_value = self.attribute_selector(data)
            # TODO (john): Is there a way to update state with a single
            # dictionary lookup?
            try:
                old_value = self.state[key]
                new_value = self.reduce_fn(old_value, new_value)
                self.state[key] = new_value
            except KeyError:  # Key does not exist in state
                self.state.setdefault(key, new_value)
            self.output._push((key, new_value))

    # Returns the local state of the operator instance
    def state(self):
        return self.state


@ray.remote
class KeyBy(OperatorInstance):
    """A key_by operator instance that physically partitions the
    stream based on a key.

    The key_by actor transforms the input data stream or records into a new
    stream of tuples of the form (key, record).

    Attributes:
        key_attribute (int): The index of the value to reduce
        (assuming tuple records).
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        # Set the key selector
        self.key_selector = operator_metadata.key_selector
        if isinstance(self.key_selector, int):
            self.key_index = self.key_selector
            self.key_selector = self._index_based_selector
        elif isinstance(self.key_selector, str):
            self.key_attribute = self.key_selector
            self.key_selector = self._attribute_based_selector
        elif not isinstance(self.key_selector, types.FunctionType):
            raise Exception("Unrecognized or unsupported key selector.")

    # Extracts the shuffling key from each record in a batch
    def _apply(self, batch, input_channel_id=None):
        batch[:] = [(self.key_selector(record), record) for record in batch]
        self.output._push_batch(batch)


# A custom source actor
@ray.remote
class Source(OperatorInstance):
    """A source emitting records.

    A user-defined source object must implement the following methods:
        - init()
        - get_next(batch_size)
        - close()
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        # The user-defined source
        self.source = operator_metadata.source
        self.source.init()  # Initialize the source
        # The watermark interval in ms
        self.watermark_interval = operator_metadata.watermark_interval
        self.max_event_time = 0  # Max event timestamp seen so far

    def __generate_watermark(self, record_batch):
        """Generates one or more watermarks.

        This method checks if a watermark must be emitted by the source based
        on the event times seen so far and a user-defined watermark interval.

        Attributes:
            record_batch (list): The list of input records, each one with
            an associated event time.
        """
        max_timestamp = 0
        for record in record_batch:
            try:
                event_time = record["dateTime"]
            except KeyError as e:
                raise Exception(e)
            max_timestamp = max(event_time, self.max_event_time)
        if (max_timestamp >= self.max_event_time + self.watermark_interval):
            # Emit watermark
            log_message = "Source emitting watermark {} due to {}"
            log_message += "on interval {}"
            logger.debug(
                log_message.format(self.max_event_time, max_timestamp,
                                   self.watermark_interval))
            # Use obj.__dict__ instead of the object itself
            self.output._broadcast_watermark(
                Watermark(self.max_event_time, time.time()).__dict__)
            # Update max event time seen so far
            self.max_event_time = max_timestamp

    # Starts the source
    def start(self):
        signal.send(ActorStart(self.instance_id))
        self.start_time = time.time()
        batch_size = self.output.max_batch_size
        while True:
            record_batch = self.source.get_next(batch_size)
            if record_batch is None:  # Source is exhausted
                logger.info("Source throuhgput: {}".format(
                    self.num_records_seen / (time.time() - self.start_time)))
                self.output._close()  # Flush and close output
                signal.send(ActorExit(self.instance_id))  # Send exit signal
                self.source.close()
                return
            self.output._push_batch(record_batch)
            # TODO (john): Handle the case a record does not have event time
            # logger.debug(
            #   "Source watermark: {}. Last record time: {}".format(
            #         self.max_event_time, record_batch[-1]["dateTime"]))
            if self.watermark_interval > 0:  # Watermarks are activated
                self.__generate_watermark(record_batch)
            # Measure source throughput every 10K records
            self.num_records_seen += len(record_batch)
            if self.num_records_seen % 10000 == 0:
                logger.info("Source throughput: {}".format(
                    self.num_records_seen / (time.time() - self.start_time)))


# A custom sink actor
@ray.remote
class Sink(OperatorInstance):
    """A dataflow sink.

    A user-defined sink object must implement the following method:
        - evict(record)
    """

    def __init__(self, instance_id, operator_metadata, input_gate, output_gate,
                 checkpoint_dir):
        OperatorInstance.__init__(self, instance_id, operator_metadata,
                                  input_gate, output_gate, checkpoint_dir)
        # The user-defined sink
        self.sink = operator_metadata.sink

    # Applies the sink logic to a batch of records
    def _apply(self, batch, input_channel_id=None):
        for record in batch:
            self.sink.evict(record)

    # Applies the sink logic to a batch of records
    def _apply_batch(self, batch, input_channel_id=None):
        for record in batch:
            self.sink.evict(record)

    # Returns the local state (if any) of the sink instance
    def state(self):
        try:  # There might be no get_state() method implemented
            return (self.instance_id, self.sink.get_state())
        except AttributeError as e:
            raise Exception(e)
