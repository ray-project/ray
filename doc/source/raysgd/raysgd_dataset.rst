Distributed Dataset
===================

The RaySGD ``Dataset`` provides a simple abstraction for training with
distributed data.

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/26EMwdahdgm7Lscy9>`_!

Setting up a dataset
--------------------

A dataset can be constructed via any python iterable, or a ``ParallelIterator``. Optionally, a batch size, download function, concurrency, and a transformation can also be specified.

A dataset 
