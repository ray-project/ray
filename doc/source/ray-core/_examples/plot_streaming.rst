Streaming MapReduce
===================

This document walks through how to implement a simple streaming application
using Ray's actor capabilities. It implements a streaming MapReduce which
computes word counts on wikipedia articles.

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/source/ray-core/_examples/streaming

To run the example, you need to install the dependencies

.. code-block:: bash

  pip install wikipedia


and then execute the script as follows:

.. code-block:: bash

  python ray/doc/source/ray-core/_examples/streaming/streaming.py

For each round of articles read, the script will output
the top 10 words in these articles together with their word count:

.. code-block:: text

  article index = 0
     the 2866
     of 1688
     and 1448
     in 1101
     to 593
     a 553
     is 509
     as 325
     are 284
     by 261
  article index = 1
     the 3597
     of 1971
     and 1735
     in 1429
     to 670
     a 623
     is 578
     as 401
     by 293
     for 285
  article index = 2
     the 3910
     of 2123
     and 1890
     in 1468
     to 658
     a 653
     is 488
     as 364
     by 362
     for 297
  article index = 3
     the 2962
     of 1667
     and 1472
     in 1220
     a 546
     to 538
     is 516
     as 307
     by 253
     for 243
  article index = 4
     the 3523
     of 1866
     and 1690
     in 1475
     to 645
     a 583
     is 572
     as 352
     by 318
     for 306
  ...

Note that this examples uses `distributed actor handles`_, which are still
considered experimental.

.. _`distributed actor handles`: http://docs.ray.io/en/master/actors.html

There is a ``Mapper`` actor, which has a method ``get_range`` used to retrieve
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

The ``Reducer`` actor holds a list of mappers, calls ``get_range`` on them
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
  keys = # Partition the keys among the reducers.

  # Create a number of mappers.
  mappers = [Mapper.remote(stream) for stream in streams]

  # Create a number of reduces, each responsible for a different range of keys.
  # This gives each Reducer actor a handle to each Mapper actor.
  reducers = [Reducer.remote(key, *mappers) for key in keys]

  article_index = 0
  while True:
      counts = ray.get([reducer.next_reduce_result.remote(article_index)
                        for reducer in reducers])
      article_index += 1

The actual example reads a list of articles and creates a stream object which
produces an infinite stream of articles from the list. This is a toy example
meant to illustrate the idea. In practice we would produce a stream of
non-repeating items for each mapper.
