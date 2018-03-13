Pandas on Ray
======

Pandas on Ray is an early stage DataFrame library that wraps Pandas and
transparently distributes the data and computation. The user does not need to
know how many cores their system has, nor do they need to specify how to
distribute the data. In fact, users can continue using their previous Pandas
notebooks while experiencing a considerable speedup from Pandas on Ray, even
on a single machine. Only a modification of the import statement is needed, as
we demonstrate below. Once you’ve changed your import statement, you’re ready
to use Pandas on Ray just like you would Pandas.

.. code-block:: python

  #import pandas as pd
  import ray.dataframe as pd

Currently, we have part of the Pandas API implemented and are working toward
full functional parity with Pandas.

Using Pandas on Ray on a Single Node
------------------------------

In order to use the most up-to-date version of Pandas on Ray, please follow
the instructions on the `installation page`_

Once you import the library, you should see something similar to the following
output:

.. code-block:: python

  >>> import ray.dataframe as pd

  Waiting for redis server at 127.0.0.1:14618 to respond...
  Waiting for redis server at 127.0.0.1:31410 to respond...
  Starting local scheduler with the following resources: {'CPU': 4, 'GPU': 0}.

  ======================================================================
  View the web UI at http://localhost:8889/notebooks/ray_ui36796.ipynb?token=ac25867d62c4ae87941bc5a0ecd5f517dbf80bd8e9b04218
  ======================================================================

If you do not see output similar to the above, please make sure that you have
built Ray using the instructions on the `installation page`_

One you have executed  ``import ray.dataframe as pd``, you're ready to begin
running your Pandas pipeline as you were before. Please note, the API is not
yet complete. For some methods, you may see the following:

.. code-block:: python

  NotImplementedError: To contribute to Pandas on Ray, please visit github.com/ray-project/ray.

If you would like to request a particular method be implemented, feel free to
`open an issue`_. Before you open an issue please make sure that someone else
has not already requested that functionality.

Using Pandas on Ray on a Cluster
------------------------------

Currently, we do not yet support running Pandas on Ray on a Cluster. Coming
Soon!

.. _`installation page`: http://ray.readthedocs.io/en/latest/installation.html
.. _`open an issue`: http://github.com/ray-project/ray/issues