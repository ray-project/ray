Distributed Dataset
===================

The RaySGD ``Dataset`` provides a simple abstraction for training with
distributed data.

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/26EMwdahdgm7Lscy9>`_!

Setting up a dataset
--------------------

A dataset can be constructed via any python iterable, or a ``ParallelIterator``. Optionally, a batch size, download function, concurrency, and a transformation can also be specified.

When constructing a dataset, a download function can be specified. For example, if a dataset is initialized with a set of paths, a download function can be specified which converts those paths to ``(input, label)`` tuples. The download function can be executed in parallel via ``max_concurrency``. This may be useful if the backing datastore has rate limits, there is high overhead associated with a download, or downloading is computationally expensive. Downloaded data is stored as objects in the plasma store.

An additional, final transformation can be specified via ``Dataset::transform``. This function is guaranteed to take place on the same worker that training will take place on. It is good practice to do operations which produce large outputs, such as converting images to tensors as transformations.

Finally, the batch size can be specified. The batch size is the number of data points used per training step per worker.

.. note:: Batch size should be specified via the dataset's constructor, __not__ the ``config["batch_size"]`` passed into the Trainer constructor. In general, datasets are configured via their own constructor, not the Trainer config, wherever possible.

Using a dataset
---------------

To use a dataset, pass it in as an argument to ``trainer.train()``. A dataset passed in to ``trainer.train`` will take precedence over the trainer's data creator during that training run.

.. code-block:: python

    trainer.train(dataset=dataset, num_steps=10) # Trains using a dataset
    trainer.train() # Trains with the original data creator
    trainer.train(dataset=dataset2, num_steps=20) # Trains using a different dataset

Sharding and Sampling
---------------------

.. note:: These details may change in the future.

Datasets use ParallelIterator actors for sharding. In order to handle datasets which do not shard evenly, and streaming datasets (which may not have a defined size), shards are represented as repeated sequences of data. As a result, num_steps should always be specified when training and some data may be oversampled if the data cannot be evenly sharded.

If the dataset is of a known length (and can be evenly sharded), training for an epoch is eqivalent to setting ``num_steps = len(data) / (num_workers * batch_size)``.

Complete dataset example
------------------------

Below is an example of training a network with a single hidden layer to learn the identity function.

.. literalinclude:: ../../../python/ray/util/sgd/data/examples/mlp_identity.py
   :language: python
