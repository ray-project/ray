Streaming MapReduce
===================

This document walks through how to implement a simple streaming application
using Ray's actor capabilities. It implements a streaming MapReduce which
computes word counts on wikipedia articles.

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/streaming

The example can be run as follows.

.. code-block:: bash

  python ray/examples/streaming/streaming.py

Note that this examples uses distributed actor handles, which are still
considered experimental.

There is a `Mapper` actor, which has a method `get_range` used to retrieve
word counts for words in a certain range:

```
@ray.remote
class Mapper(object):

    def __init__(self, titles):
        # Constructor, the titles parameter is a list of wikipedia
        # article titles that will be read by this mapper

    def get_range(self, article_index, keys):
        # Return counts of all the words with first
        # letter between keys[0] and keys[1] in the
        # articles with index up to article_index
```

The `Reducer` actor holds a list of mappers, calls `get_range` on them
and accumulates the results.

```
@ray.remote
class Reducer(object):

    def __init__(self, keys, *mappers):
         # Constructor for a reducer that gets input from the list of mappers
         # in the argument and accumulates word counts for words with first
         # letter between keys[0] and keys[1]

    def next_reduce_result(self, article_index):
         # Get articles up to article_index in the reducers,
         # accumulate the word counts and return them
```

On the driver, we then create a number of mappers and reducers and run the
streaming MapReduce:

```
cities = ["New York City", "Berlin", "London", "Paris"]
countries = ["United States", "Germany", "France", "United Kingdom"]

mappers = [Mapper.remote(cities), Mapper.remote(countries)]

reducers = [Reducer.remote(["a", "m"], *mappers),
            Reducer.remote(["n", "z"], *mappers)]

for article_index in range(4):
    counts = ray.get([
        reducers[0].next_reduce_result.remote(article_index),
        reducers[1].next_reduce_result.remote(article_index)])
    print("counts:", counts)
```
