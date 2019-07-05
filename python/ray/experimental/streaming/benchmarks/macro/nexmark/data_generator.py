from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Watermark

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# A stream replayer that reads Nexmark events
# from files and replays them at given rates
class NexmarkEventGenerator(object):
    def __init__(self, event_file, event_type, event_rate,
                       sample_period=1000, max_records=-1,
                       omit_extra=False):
        self.event_file = event_file
        self.max_records = max_records if max_records > 0 else float("inf")
        self.event_rate = event_rate  if event_rate > 0 else float("inf")
        self.event_type = event_type  # Auction, Bid, Person
        assert event_type in ["Auction","Bid","Person"]
        self.events = []
        self.omit_extra_field = omit_extra

        # Used for event replaying
        self.total_count = 0  # Total number of records emmitted so far
        self.count = 0        # Number of records emmitted since last sampling
        self.period = sample_period  # The latency sampling period
        self.start = 0        # Start timestamp of the generator
        self.done = False     # Denotes whetehr the source is exhausted or not

    # Parses a nexmark event log and creates an event object
    def __create_event(self, event, omit_extra_field=False):
        obj = Bid() if self.event_type == "Bid" else Person(
                            ) if self.event_type == "Person" else Auction()
        event = event.strip()[1:-1]  # Trim spaces and brackets
        raw_attributes = event.split(",")
        attribute_value = []
        for attribute in raw_attributes:
            k_v = attribute.split(":")
            key = k_v[0][1:-1]
            value = int(k_v[1]) if k_v[1][0] != "\"" else str(k_v[1])
            if (key != "extra") or (not omit_extra_field):
                setattr(obj, key, value)
        # Return the event's fields in the form of a dictionary
        return obj.__dict__

    # Used to rate limit the source
    def __wait(self):
        while (self.total_count / (time.time() - self.start) >
               self.event_rate):
           time.sleep(0.00005)  # 50 us

    # Loads input file
    def init(self):
        # Load all events from the input file in memory
        logger.info("Loading input file in memory...")
        records = 0
        with open(self.event_file, "r") as ef:
            for event in ef:
                self.events.append(self.__create_event(event,
                                    self.omit_extra_field))
                records += 1
                if records == self.max_records:
                    break
        logger.info("Done.")

    # Returns the next batch of events for the given size
    def get_next(self, batch_size=1):
        if not self.start:
            # Start time is used in __wait() to measure source throughput
            self.start = time.time()
        if (self.total_count == len(self.events) or  # Exhausted
            self.total_count >= self.max_records):   # Reached max events
            return None  # Exhausted
        limit = min(len(self.events), self.total_count + batch_size)
        event_batch = self.events[self.total_count:limit]
        added_records = limit - self.total_count
        self.total_count += added_records
        self.__wait()  # Wait if needed to meet the desired throughput
        self.count += added_records
        if self.count >= self.period:
            self.count = 0
            # Assign the generation timestamp to the 1st record of the batch
            # TODO (john): Should we assign this timestamp randomly?
            event_batch[0]["system_time"] = time.time()
        return event_batch

    # Closes the generator
    def close(self):
        pass


# A custom sink used to measure end-to-end processing latency
class EventLatencySink(object):
    def __init__(self):
        self.state = []

    # Evicts next batch of records
    def evict(self, batch):
        for record in batch:
            if record["event_type"] == "Watermark":
                return  # Ignore watermarks
            generation_time = record["system_time"]
            if generation_time is not None:
                # TODO (john): Clock skew might distort elapsed time
                self.state.append(time.time() - generation_time)

    # Initializes the sink
    def init(self):
        pass

    # Returns sink's state
    def get_state(self):
        return self.state
