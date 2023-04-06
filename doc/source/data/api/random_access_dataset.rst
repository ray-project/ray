.. _random-access-dataset-api:

(Experimental) RandomAccessDataset API
======================================

.. currentmodule:: ray.data

RandomAccessDataset objects are returned by call: Dataset.to_random_access_dataset().

Constructor
-----------

.. autosummary::
   :toctree: doc/

   random_access_dataset.RandomAccessDataset

Functions
---------

.. autosummary::
   :toctree: doc/

   random_access_dataset.RandomAccessDataset.get_async
   random_access_dataset.RandomAccessDataset.multiget
   random_access_dataset.RandomAccessDataset.stats
