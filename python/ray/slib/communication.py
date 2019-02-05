import sys

from ray.slib.streaming_operator import PStrategy
from ray.slib.batched_queue import BatchedQueue

# A data channel is a batched queue between two operator instances in a streaming environment
class DataChannel(object):
    def __init__(self, env,
                        src_operator_id,
                        dst_operator_id,
                        src_instance_id,
                        dst_instance_id):
        self.env = env
        self.src_operator_id = src_operator_id
        self.dst_operator_id = dst_operator_id
        self.src_instance_id = src_instance_id
        self.dst_instance_id = dst_instance_id
        self.queue = BatchedQueue(max_size=self.env.config.queue_config.max_size,
                        max_batch_size=self.env.config.queue_config.max_batch_size,
                        max_batch_time=self.env.config.queue_config.max_batch_time,
                        prefetch_depth=self.env.config.queue_config.prefetch_depth,
                        background_flush=self.env.config.queue_config.background_flush)

    def __repr__(self):
        return "({},{},{},{})".format(self.src_operator_id,self.dst_operator_id,self.src_instance_id,self.dst_instance_id)

# Pulls and merges data from multiple input channels
class DataInput(object):
    def __init__(self, channels):
        self.input_channels = channels
        self.channel_index = 0
        self.max_index = len(channels)

    # Fetches records from input channels in a round-robin fashion
    # TODO (john): Make sure the instance is not blocked on any of its input channels
    # TODO (john): In case of input skew, it might be better to prefer pulling from the largest queue more often
    def pull(self):
        c = self.input_channels[self.channel_index] # Channel to pull from
        self.channel_index += 1
        if self.channel_index == self.max_index: self.channel_index = 0
        res =  c.queue.read_next()
        print("[reader] Pulled record '{}'".format(res))
        return res

# Selects output channel(s) and pushes data
class DataOutput(object):
    def __init__(self, channels, partitioning_schemes):
        self.partitioning_schemes = partitioning_schemes
        self.output_channels = channels
        self.total_records_written = 0
        # Collect forward and broadcast channels
        self.fb_channels = []
        for c in self.output_channels:
            s = self.partitioning_schemes[c.dst_operator_id].strategy
            if s == PStrategy.Forward or s == PStrategy.Broadcast:
                self.fb_channels.append(c)
        # Collect shuffle channels
        slots = sum(1 for n in self.partitioning_schemes.values() if n.strategy==PStrategy.Shuffle)
        self.shuffle_exists = slots > 0
        self.shuffle_channels = [[]] * slots
        destinations = {}   # Distinct shuffle destinations
        index = 0;
        for c in self.output_channels:
            s = self.partitioning_schemes[c.dst_operator_id].strategy
            if s == PStrategy.Shuffle:
                slot = destinations.setdefault(c.dst_operator_id,index)
                self.shuffle_channels[slot].append(c)
                if slot == index: index += 1
            elif s != PStrategy.Forward and s != PStrategy.Broadcast:
                # TODO (john): Add support for other partitioting strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")

    # Pushes the record to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def push(self, record):
        if not record:  # Flush remaining records to object store
            for c in self.output_channels:
                c.queue.put_next(record)    # Propagate 'None'
                c.queue._flush_writes()
            return
        # Forward record
        for c in self.fb_channels:
            print("[writer] Push record '{}' to channel {}".format(record,c))
            c.queue.put_next(record)
        # Hash-based shuffling per destination
        if self.shuffle_exists:
            h = hash(record)    # TODO (john): Should be able to shuffle by a key
            for cs in self.shuffle_channels:
                l = len(cs)     # Number of downstream instances
                c = cs[h % l]
                print("[writer] Push record '{}' to channel {}".format(record,c))
                c.queue.put_next(record)

    # Pushes a list of records to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def push_all(self, records):
        for r in records: print("[writer] Push record '{}'".format(r))
        if not records: # Flush remaining records to object store
            for c in self.output_channels:
                c.queue.put_next(None)  # Propagate 'None'
                c.queue._flush_writes()
            return
        # Forward records
        for r in records:
            for c in self.fb_channels:
                print("[writer] Push record '{}' to channel {}".format(r,c))
                c.queue.put_next(r)
        # Hash-based shuffling per destination
        if self.shuffle_exists:
            for r in records:
                h = hash(r)         # TODO (john): Should be able to shuffle by a key
                for cs in self.shuffle_channels:
                    l = len(cs)     # Number of downstream instances
                    c = cs[h % l]
                    print("[writer] Push record '{}' to channel {}".format(r,c))
                    c.queue.put_next(r)

# Batched queue configuration
class QueueConfig(object):
    def __init__(self):
        self.max_size=999999
        self.max_batch_size=99999
        self.max_batch_time=0.01
        self.prefetch_depth=10
        self.background_flush=False
