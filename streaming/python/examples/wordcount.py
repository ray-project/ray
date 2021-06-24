import argparse
import logging
import sys
import time

import ray
import wikipedia
from ray.streaming import StreamingContext
from ray.streaming.config import Config

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
class Wikipedia:
    def __init__(self, title_file):
        # Titles in this file will be as queries
        self.title_file = title_file
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
    return [(word, 1) for word in line.split()]


if __name__ == "__main__":
    # Get program parameters
    args = parser.parse_args()
    titles_file = str(args.titles_file)

    ray.init(job_config=ray.job_config.JobConfig(code_search_path=sys.path))

    ctx = StreamingContext.Builder() \
        .option(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL) \
        .build()
    # A Ray streaming environment with the default configuration
    ctx.set_parallelism(1)  # Each operator will be executed by two actors

    # Reads articles from wikipedia, splits them in words,
    # shuffles words, and counts the occurrences of each word.
    stream = ctx.source(Wikipedia(titles_file)) \
        .flat_map(splitter) \
        .key_by(lambda x: x[0]) \
        .reduce(lambda old_value, new_value:
                (old_value[0], old_value[1] + new_value[1])) \
        .sink(print)
    start = time.time()
    ctx.execute("wordcount")
    end = time.time()
    logger.info("Elapsed time: {} secs".format(end - start))
