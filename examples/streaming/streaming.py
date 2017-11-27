from collections import Counter, defaultdict
import heapq
import wikipedia
import ray

ray.init()

@ray.remote
class Mapper(object):
    def __init__(self, titles):
        self.titles = titles
        self.articles = []
        self.word_counts = []
        self.words = []

    def get_new_article(self):
        article_index = len(self.articles)
        self.articles.append(wikipedia.page(self.titles[article_index]).content)
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

cities = ["New York City", "Berlin", "London", "Paris"]
countries = ["United States", "Germany", "France", "United Kingdom"]

mappers = [Mapper.remote(cities), Mapper.remote(countries)]

reducers = [Reducer.remote(["a", "m"], *mappers),
            Reducer.remote(["n", "z"], *mappers)]

for article_index in range(4):
    print("article index =", article_index)
    wordcounts = dict()
    counts1, counts2 = ray.get([
                   reducers[0].next_reduce_result.remote(article_index),
                   reducers[1].next_reduce_result.remote(article_index)])
    wordcounts.update(counts1)
    wordcounts.update(counts2)
    most_frequent_words = heapq.nlargest(10, wordcounts, key=wordcounts.get)
    for word in most_frequent_words:
        print(word, wordcounts[word])
