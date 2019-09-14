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


# Test functions
def splitter(line):
    return line.split()


def filter_fn(word):
    if "f" in word:
        return True
    return False


if __name__ == "__main__":

    args = parser.parse_args()

    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)
    ray.register_custom_serializer(OpType, use_pickle=True)
    ray.register_custom_serializer(PStrategy, use_pickle=True)

    # A Ray streaming environment with the default configuration
    env = Environment()

    # Stream represents the ouput of the filter and
    # can be forked into other dataflows
    stream = env.read_text_file(args.input_file) \
                .shuffle() \
                .flat_map(splitter) \
                .set_parallelism(4) \
                .filter(filter_fn) \
                .set_parallelism(2) \
                .inspect(print)     # Prints the contents of the
    # stream to stdout
    start = time.time()
    env_handle = env.execute()
    ray.get(env_handle)  # Stay alive until execution finishes
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
