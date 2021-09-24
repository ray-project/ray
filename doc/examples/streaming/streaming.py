import argparse
from collections import Counter, defaultdict
import heapq
import numpy as np
import os
import ray
import wikipedia

parser = argparse.ArgumentParser()
parser.add_argument(
    "--num-mappers", help="number of mapper actors used", default=3, type=int)
parser.add_argument(
    "--num-reducers",
    help="number of reducer actors used",
    default=4,
    type=int)


@ray.remote
class Mapper(object):
    def __init__(self, title_stream):
        self.title_stream = title_stream
        self.num_articles_processed = 0
        self.articles = []
        self.word_counts = []

    def get_new_article(self):
        # Get the next wikipedia article.
        article = wikipedia.page(self.title_stream.next()).content
        # Count the words and store the result.
        self.word_counts.append(Counter(article.split(" ")))
        self.num_articles_processed += 1

    def get_range(self, article_index, keys):
        # Process more articles if this Mapper hasn't processed enough yet.
        while self.num_articles_processed < article_index + 1:
            self.get_new_article()
        # Return the word counts from within a given character range.
        return [(k, v) for k, v in self.word_counts[article_index].items()
                if len(k) >= 1 and k[0] >= keys[0] and k[0] <= keys[1]]


@ray.remote
class Reducer(object):
    def __init__(self, keys, *mappers):
        self.mappers = mappers
        self.keys = keys

    def next_reduce_result(self, article_index):
        word_count_sum = defaultdict(lambda: 0)
        # Get the word counts for this Reducer's keys from all of the Mappers
        # and aggregate the results.
        count_ids = [
            mapper.get_range.remote(article_index, self.keys)
            for mapper in self.mappers
        ]

        while len(count_ids) > 0:
            finished, unfinished = ray.wait(count_ids)
            for count_id in finished:
                for k, v in ray.get(count_id):
                    word_count_sum[k] += v
            count_ids = unfinished

        return word_count_sum


class Stream(object):
    def __init__(self, elements):
        self.elements = elements

    def next(self):
        i = np.random.randint(0, len(self.elements))
        return self.elements[i]


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    # Create one streaming source of articles per mapper.
    directory = os.path.dirname(os.path.realpath(__file__))
    streams = []
    for _ in range(args.num_mappers):
        with open(os.path.join(directory, "articles.txt")) as f:
            streams.append(Stream([line.strip() for line in f.readlines()]))

    # Partition the keys among the reducers.
    chunks = np.array_split([chr(i)
                             for i in range(ord("a"),
                                            ord("z") + 1)], args.num_reducers)
    keys = [[chunk[0], chunk[-1]] for chunk in chunks]

    # Create a number of mappers.
    mappers = [Mapper.remote(stream) for stream in streams]

    # Create a number of reduces, each responsible for a different range of
    # keys. This gives each Reducer actor a handle to each Mapper actor.
    reducers = [Reducer.remote(key, *mappers) for key in keys]

    article_index = 0
    while True:
        print("article index = {}".format(article_index))
        wordcounts = {}
        counts = ray.get([
            reducer.next_reduce_result.remote(article_index)
            for reducer in reducers
        ])
        for count in counts:
            wordcounts.update(count)
        most_frequent_words = heapq.nlargest(
            10, wordcounts, key=wordcounts.get)
        for word in most_frequent_words:
            print("  ", word, wordcounts[word])
        article_index += 1
