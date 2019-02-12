import sys

from ray.experimental.slib.operator import PStrategy
from ray.experimental.slib.batched_queue import BatchedQueue

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
        self.closed = [False] * len(self.input_channels)  # Tracks the channels that have been closed
        self.all_closed = False

    # Fetches records from input channels in a round-robin fashion
    # TODO (john): Make sure the instance is not blocked on any of its input channels
    # TODO (john): In case of input skew, it might be better to pull from the largest queue more often
    def _pull(self):
        while True:
            c = self.input_channels[self.channel_index] # Channel to pull from
            self.channel_index += 1
            if self.channel_index == self.max_index: self.channel_index = 0
            if self.closed[self.channel_index-1]: continue      # Channel has been 'closed', check next
            record = c.queue.read_next()
            #print("Actor ({},{}) pulled '{}'.".format(c.src_operator_id,c.src_instance_id,record))
            if record == None:  # Mark channel as 'closed' and pull from the next open one
                self.closed[self.channel_index-1] = True
                self.all_closed = True
                for s in self.closed:
                    if not s:
                        self.all_closed = False
                        break
                if not self.all_closed: continue
            # Returns 'None' iff all input channels are 'closed'
            return record

# Selects output channel(s) and pushes data
class DataOutput(object):
    def __init__(self, channels, partitioning_schemes):
        self.partitioning_schemes = partitioning_schemes
        self.total_records_written = 0
        # Prepare output -- collect channels by type
        self.fb_channels = []   # Forward and broadcast channels
        slots = sum(1 for n in self.partitioning_schemes.values() if n.strategy==PStrategy.Shuffle)
        self.shuffle_exists = slots > 0         # Flag used to avoid hashing when there is no shuffling
        self.shuffle_channels = [[]] * slots    # Shuffle channels
        slots = sum(1 for n in self.partitioning_schemes.values() if n.strategy==PStrategy.ShuffleByKey)
        self.shuffle_key_exists = slots > 0         # Flag used to avoid hashing when there is no shuffling by key
        self.shuffle_key_channels = [[]] * slots    # Shuffle by key channels
        shuffle_destinations = {}           # Distinct shuffle destinations
        shuffle_by_key_destinations = {}    # Distinct shuffle by key destinations
        index_1 = 0;
        index_2 = 0;
        for c in channels:
            s = self.partitioning_schemes[c.dst_operator_id].strategy
            if s == PStrategy.Forward or s == PStrategy.Broadcast:
                self.fb_channels.append(c)
            elif s == PStrategy.Shuffle:
                slot = shuffle_destinations.setdefault(c.dst_operator_id,index_1)
                self.shuffle_channels[slot].append(c)
                if slot == index_1: index_1 += 1
            elif s == PStrategy.ShuffleByKey:
                slot = shuffle_by_key_destinations.setdefault(c.dst_operator_id,index_2)
                self.shuffle_key_channels[slot].append(c)
                if slot == index_2: index_2 += 1
            else: # TODO (john): Add support for other partitioting strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        # A KeyedDataStream can only be shuffled by key
        assert(not(self.shuffle_exists and self.shuffle_key_exists))

    # Flushes any remaining records in the output channels
    # 'close' indicates whether we should also 'close' the channel (True) by propagating 'None'
    # or just flush the remaining records to plasma (False)
    def _flush(self,close=False):
        for c in self.fb_channels:
            if close == True: c.queue.put_next(None)
            c.queue._flush_writes()
        for cs in self.shuffle_channels:
            for c in cs:
                if close == True: c.queue.put_next(None)
                c.queue._flush_writes()
        for cs in self.shuffle_key_channels:
            for c in cs:
                if close == True: c.queue.put_next(None)
                c.queue._flush_writes()
        # TODO (john): Add more channel types

    # Returns all destination actor ids
    def _destination_actor_ids(self):
        destinations = []
        for c in self.fb_channels:
            destinations.append((c.dst_operator_id,c.dst_instance_id))
        for cs in self.shuffle_channels:
            for c in cs: destinations.append((c.dst_operator_id,c.dst_instance_id))
        for cs in self.shuffle_key_channels:
            for c in cs: destinations.append((c.dst_operator_id,c.dst_instance_id))
        # TODO (john): Add more channel types
        return destinations

    # Pushes the record to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def _push(self, record):
        # Forward record
        for c in self.fb_channels:
            # print("[writer] Push record '{}' to channel {}".format(record,c))
            c.queue.put_next(record)
        # Hash-based shuffling by key
        if self.shuffle_key_exists:
            k,_ = record
            h = hash(k)
            for cs in self.shuffle_key_channels:
                l = len(cs)     # Number of downstream instances
                c = cs[h % l]
                #print("[writer] Push record '{}' to channel {}".format(record,c))
                c.queue.put_next(record)
        elif self.shuffle_exists:   # Hash-based shuffling per destination
            h = hash(record)
            for cs in self.shuffle_channels:
                l = len(cs)     # Number of downstream instances
                c = cs[h % l]
                # print("[writer] Push record '{}' to channel {}".format(record,c))
                c.queue.put_next(record)
        else: # TODO (john): Handle rescaling
            pass

    # Pushes a list of records to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def _push_all(self, records):
        # Forward records
        for r in records:
            for c in self.fb_channels:
                # print("[writer] Push record '{}' to channel {}".format(r,c))
                c.queue.put_next(r)
        # Hash-based shuffling by key per destination
        if self.shuffle_key_exists:
            for r in records:
                k,_ = r
                h = hash(k)
                for cs in self.shuffle_channels:
                    l = len(cs)     # Number of downstream instances
                    c = cs[h % l]
                    # print("[writer] Push record '{}' to channel {}".format(r,c))
                    c.queue.put_next(r)
        elif self.shuffle_exists: # Hash-based shuffling per destination
            for r in records:
                h = hash(r)
                for cs in self.shuffle_channels:
                    l = len(cs)     # Number of downstream instances
                    c = cs[h % l]
                    # print("[writer] Push record '{}' to channel {}".format(r,c))
                    c.queue.put_next(r)
        else: # TODO (john): Handle rescaling
            pass

# Batched queue configuration
class QueueConfig(object):
    def __init__(self):
        self.max_size=999999
        self.max_batch_size=99999
        self.max_batch_time=0.01
        self.prefetch_depth=10
        self.background_flush=False
