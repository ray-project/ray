Streaming MapReduce
===================

This document walks through how to implement a simple streaming application
using Ray's actor capabilities. It implements a streaming MapReduce which
computes word counts on wikipedia articles.

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/streaming

To run the example, you need to install the dependencies

.. code-block:: bash

  pip install wikipedia


and then execute the script as follows:

.. code-block:: bash

  python ray/examples/streaming/streaming.py

For each round of articles read, the script will output
the top 10 words in these articles together with their word count:

.. code-block::

  article index = 0
  and 1507
  in 1221
  a 565
  is 446
  by 263
  as 260
  for 237
  from 230
  are 224
  has 163
  article index = 1
  and 1781
  in 1473
  a 635
  is 590
  as 417
  by 337
  are 317
  for 311
  has 225
  from 191
  article index = 2
  and 1893
  in 1695
  a 666
  is 438
  de 426
  from 357
  by 354
  for 333
  city 273
  its 261
  article index = 3
  ...

Note that this examples uses distributed actor handles, which are still
considered experimental.

There is a `Mapper` actor, which has a method `get_range` used to retrieve
word counts for words in a certain range:

.. code-block:: python

  @ray.remote
  class Mapper(object):

      def __init__(self, title_stream):
          # Constructor, the title stream parameter is a stream of wikipedia
          # article titles that will be read by this mapper

      def get_range(self, article_index, keys):
          # Return counts of all the words with first
          # letter between keys[0] and keys[1] in the
          # articles that haven't been read yet with index
          # up to article_index

The `Reducer` actor holds a list of mappers, calls `get_range` on them
and accumulates the results.

.. code-block:: python

  @ray.remote
  class Reducer(object):

      def __init__(self, keys, *mappers):
           # Constructor for a reducer that gets input from the list of mappers
           # in the argument and accumulates word counts for words with first
           # letter between keys[0] and keys[1]

      def next_reduce_result(self, article_index):
           # Get articles up to article_index that haven't been read yet,
           # accumulate the word counts and return them

On the driver, we then create a number of mappers and reducers and run the
streaming MapReduce:

.. code-block:: python

  streams = # Create list of num_mappers streams

  mappers = [Mapper.remote(stream) for stream in streams]

  chunks = np.array_split([chr(i) for i in range(ord('a'), ord('z') + 1)],
                          num_reducers)

  reducers = [Reducer.remote([chunk[0], chunk[-1]], *mappers)
              for chunk in chunks]

  article_index = 0
  while True:
      print("article index =", article_index)
      counts = ray.get([reducer.next_reduce_result.remote(article_index)
                        for reducer in reducers])
      print("counts:", counts)
      article_index += 1

The actual example reads a list of articles and creates a Stream class which
produces an infinite stream of articles from the list. This is a toy example
meant to illustrate the idea. In practice we would produce a streams of
non-repeating items for each mapper.
