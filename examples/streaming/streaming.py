import argparse
from collections import Counter, defaultdict
import heapq
import itertools
import numpy as np
import random
import ray
import os
import wikipedia

@ray.remote
class Mapper(object):
    def __init__(self, title_stream):
        self.title_stream = title_stream
        self.articles = []
        self.word_counts = []
        self.words = []

    def get_new_article(self):
        self.articles.append(wikipedia.page(self.title_stream.next()).content)
        self.word_counts.append(Counter(self.articles[-1].split(" ")))
        self.words.append(sorted(self.word_counts[-1].keys()))

    def get_range(self, article_index, keys):
        while len(self.articles) < article_index + 1:
            self.get_new_article()
        return [(k, v) for k, v in self.word_counts[article_index].items()
                if len(k) >= 1 and k[0] >= keys[0] and k[0] <= keys[1]]

@ray.remote
class Reducer(object):
    def __init__(self, keys, *mappers):
        self.mappers = mappers
        self.keys = keys

    def next_reduce_result(self, article_index):
        word_count_sum = defaultdict(lambda: 0)
        for mapper in self.mappers:
            counts = mapper.get_range.remote(article_index, self.keys)
            for k, v in ray.get(counts):
                word_count_sum[k] += v
        return word_count_sum

class Stream(object):

    def __init__(self, elements):
        self.elements = elements

    def next(self):
        i = random.randint(0, len(self.elements) - 1)
        return self.elements[i]

parser = argparse.ArgumentParser()
parser.add_argument('--num-mappers',
                    help='number of mapper actors used', default=3)
parser.add_argument('--num-reducers',
                    help='number of reducer actors used', default=4)
args = parser.parse_args()

if __name__ == "__main__":
    ray.init()

    directory = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(directory, "articles.txt")) as f:
        stream = Stream([line.strip() for line in f.readlines()])

    mappers = [Mapper.remote(stream) for i in range(args.num_mappers)]

    chunks = np.array_split([chr(i) for i in range(ord('a'), ord('z') + 1)],
                            args.num_reducers)

    reducers = [Reducer.remote([chunk[0], chunk[-1]], *mappers)
                for chunk in chunks]

    article_index = 0
    while True:
        print("article index =", article_index)
        wordcounts = dict()
        counts = ray.get([reducer.next_reduce_result.remote(article_index) for reducer in reducers])
        for count in counts:
            wordcounts.update(count)
        most_frequent_words = heapq.nlargest(10, wordcounts, key=wordcounts.get)
        for word in most_frequent_words:
            print(word, wordcounts[word])
        article_index += 1
