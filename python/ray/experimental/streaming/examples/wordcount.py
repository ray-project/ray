from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import time
import wikipedia

import ray
from ray.experimental.streaming.streaming import Environment
from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.operator import OpType, PStrategy

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--titles-file",
    required=True,
    help="the file containing the wikipedia titles to lookup")


# A custom data source that reads articles from wikipedia
# Custom data sources need to implement a get_next() method
# that returns the next data element, in this case sentences
class Wikipedia(object):
    def __init__(self, title_file):
        # Titles in this file will be as queries
        self.title_file = title_file
        # TODO (john): Handle possible exception here
        self.title_reader = iter(list(open(self.title_file, "r").readlines()))
        self.done = False
        self.article_done = True
        self.sentences = iter([])

    # Returns next sentence from a wikipedia article
    def get_next(self):
        if self.done:
            return None  # Source exhausted
        while True:
            if self.article_done:
                try:  # Try next title
                    next_title = next(self.title_reader)
                except StopIteration:
                    self.done = True  # Source exhausted
                    return None
                # Get next article
                logger.debug("Next article: {}".format(next_title))
                article = wikipedia.page(next_title).content
                # Split article in sentences
                self.sentences = iter(article.split("."))
                self.article_done = False
            try:  # Try next sentence
                sentence = next(self.sentences)
                logger.debug("Next sentence: {}".format(sentence))
                return sentence
            except StopIteration:
                self.article_done = True


# Splits input line into words and
# outputs records of the form (word,1)
def splitter(line):
    records = []
    words = line.split()
    for w in words:
        records.append((w, 1))
    return records


# Returns the first attribute of a tuple
def key_selector(tuple):
    return tuple[0]


# Returns the second attribute of a tuple
def attribute_selector(tuple):
    return tuple[1]


if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    titles_file = str(args.titles_file)

    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)
    ray.register_custom_serializer(OpType, use_pickle=True)
    ray.register_custom_serializer(PStrategy, use_pickle=True)

    # A Ray streaming environment with the default configuration
    env = Environment()
    env.set_parallelism(2)  # Each operator will be executed by two actors

    # The following dataflow is a simple streaming wordcount
    #  with a rolling sum operator.
    # It reads articles from wikipedia, splits them in words,
    # shuffles words, and counts the occurences of each word.
    stream = env.source(Wikipedia(titles_file)) \
                .round_robin() \
                .flat_map(splitter) \
                .key_by(key_selector) \
                .sum(attribute_selector) \
                .inspect(print)     # Prints the contents of the
    # stream to stdout
    start = time.time()
    env_handle = env.execute()  # Deploys and executes the dataflow
    ray.get(env_handle)  # Stay alive until execution finishes
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
    logger.debug("Output stream id: {}".format(stream.id))
