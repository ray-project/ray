from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time

import ray
from ray.experimental.streaming.streaming import Environment
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.operator import OpType, PStrategy

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument("--input-file", required=True, help="the input text file")


# A class used to check attribute-based key selection
class Record(object):
    def __init__(self, record):
        k, _ = record
        self.word = k
        self.record = record


# Splits input line into words and outputs objects of type Record
# each one consisting of a key (word) and a tuple (word,1)
def splitter(line):
    records = []
    words = line.split()
    for w in words:
        records.append(Record((w, 1)))
    return records


# Receives an object of type Record and returns the actual tuple
def as_tuple(record):
    return record.record


if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    input_file = str(args.input_file)

    ray.init()
    ray.register_custom_serializer(Record, use_dict=True)
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)
    ray.register_custom_serializer(OpType, use_pickle=True)
    ray.register_custom_serializer(PStrategy, use_pickle=True)

    # A Ray streaming environment with the default configuration
    env = Environment()
    env.set_parallelism(2)  # Each operator will be executed by two actors

    # 'key_by("word")' physically partitions the stream of records
    # based on the hash value of the 'word' attribute (see Record class above)
    # 'map(as_tuple)' maps a record of type Record into a tuple
    # 'sum(1)' sums the 2nd element of the tuple, i.e. the word count
    stream = env.read_text_file(input_file) \
                .round_robin() \
                .flat_map(splitter) \
                .key_by("word") \
                .map(as_tuple) \
                .sum(1) \
                .inspect(print)     # Prints the content of the
    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
