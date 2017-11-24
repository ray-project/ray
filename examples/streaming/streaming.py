import ray
import wikipedia
from collections import Counter, defaultdict

ray.init()

@ray.remote
class Mapper(object):
    def __init__(self, title):
        self.title = title
        self.articles = []
        self.word_counts = []
        self.words = []

    def get_new_article(self):
        self.articles.append(wikipedia.page(self.title).content)
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

mappers = []
mappers.append(Mapper.remote(["New York (state)"]))
mappers.append(Mapper.remote(["Barack Obama"]))

reducers = []
reducers.append(Reducer.remote(["a", "m"], *mappers))
reducers.append(Reducer.remote(["n", "z"], *mappers))

for article_index in range(10):
    print("current result",
          ray.get([reducers[0].next_reduce_result.remote(article_index),
                   reducers[1].next_reduce_result.remote(article_index)]))
