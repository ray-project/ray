from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import logging
import argparse

from ray.experimental.slib.streaming import *

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


# Splits input line into words and outputs tuples of the form (word,1)
def splitter(line):
    res = []
    words = line.split()
    for w in words:
        res.append((w, 1))
    return res


parser = argparse.ArgumentParser()
parser.add_argument("--input-file", required=True, help="the input text file")

if __name__ == '__main__':

    args = parser.parse_args()

    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)
    ray.register_custom_serializer(OpType, use_pickle=True)
    ray.register_custom_serializer(PStrategy, use_pickle=True)

    env = Environment(
    )  # A Ray streaming environment with the default configuration
    env.set_parallelism(2)  # Each operator will be executed by two actors

    # Stream represents the ouput of the sum and can be forked into other dataflows
    stream = env.read_text_file(args.input_file) \
                    .shuffle() \
                    .flat_map(splitter) \
                    .key_by(0) \
                    .sum(1) \
                    .inspect(print)     # Prints the content of the stream

    start = time.time()
    env.execute()
    end = time.time()
    print("Elapsed time: {} secs".format(end - start))
