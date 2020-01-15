import argparse
import logging
import time

import ray
from ray.streaming.config import Config
from ray.streaming.streaming import Environment, Conf

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

    ray.init(local_mode=False)

    # A Ray streaming environment with the default configuration
    env = Environment(config=Conf(channel_type=Config.NATIVE_CHANNEL))

    # Stream represents the ouput of the filter and
    # can be forked into other dataflows
    stream = env.read_text_file(args.input_file) \
        .shuffle() \
        .flat_map(splitter) \
        .set_parallelism(2) \
        .filter(filter_fn) \
        .set_parallelism(2) \
        .inspect(lambda x: print("result", x))     # Prints the contents of the
    # stream to stdout
    start = time.time()
    env_handle = env.execute()
    ray.get(env_handle)  # Stay alive until execution finishes
    env.wait_finish()
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
