.. _consuming_datasets:

==================
Consuming Datasets
==================

The data underlying a ``Dataset`` can be consumed in several ways:

* Retrieving a limited prefix of rows.
* Iterating over rows and batches.
* Saving to files.

Retrieving a limited set of rows
================================

A limited set of rows can be retried from a ``Dataset`` via the
:meth:`ds.take() <ray.data.Dataset.take>` API, along with its sibling helper APIs
:meth:`ds.take_all() <ray.data.Dataset.take_all>`, for retrieving **all** rows, and
:meth:`ds.show() <ray.data.Dataset.show>`, for printing a limited set of rows. These
methods are convenient for quickly inspecting a subset (prefix) of rows. They have the
benefit that, if used right after reading, they will only trigger more files to be
read if needed to retrieve rows from that file; if inspecting a small prefix of rows,
often only the first file will need to be read.

.. literalinclude:: ./doc_code/consuming_datasets.py
  :language: python
  :start-after: __take_begin__
  :end-before: __take_end__

Iterating over Datasets
=======================

Datasets can be consumed a row at a time using the
:meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>` API

.. literalinclude:: ./doc_code/consuming_datasets.py
  :language: python
  :start-after: __iter_rows_begin__
  :end-before: __iter_rows_end__

or a batch at a time using the
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` API, where you can specify
batch size as well as the desired batch format. By default, the batch format is
``"default"``. For tabular data, the default format is a Pandas DataFrame; for Python
objects, it's a list.

.. literalinclude:: ./doc_code/consuming_datasets.py
  :language: python
  :start-after: __iter_batches_begin__
  :end-before: __iter_batches_end__


Datasets can be passed to Ray tasks or actors and accessed by these iteration methods.
This does not incur a copy, since the blocks of the Dataset are passed by reference as Ray objects:

.. literalinclude:: ./doc_code/consuming_datasets.py
  :language: python
  :start-after: __remote_iterators_begin__
  :end-before: __remote_iterators_end__


Splitting Into and Consuming Shards
===================================

Datasets can be split up into disjoint sub-datasets, or shards.
Locality-aware splitting is supported if you pass in a list of actor handles to the
:meth:`ds.split() <ray.data.Dataset.split>` function along with the number of desired splits.
This is a common pattern useful for loading and sharding data between distributed training actors:

.. note::

  If using :ref:`Ray Train <train-docs>` for distributed training, you do not need to split the dataset; Ray
  Train will automatically do locality-aware splitting into per-trainer shards for you!

.. literalinclude:: ./doc_code/consuming_datasets.py
  :language: python
  :start-after: __split_begin__
  :end-before: __split_end__

.. _saving_datasets:

Saving Datasets
===============

Datasets can be written to local or remote storage in the desired data format.
The supported formats include Parquet, CSV, JSON, NumPy. To control the number
of output files, you may use :meth:`ds.repartition() <ray.data.Dataset.repartition>`
to repartition the Dataset before writing out.

.. tabbed:: Parquet

  .. literalinclude:: ./doc_code/saving_datasets.py
    :language: python
    :start-after: __write_parquet_begin__
    :end-before: __write_parquet_end__

.. tabbed:: CSV

  .. literalinclude:: ./doc_code/saving_datasets.py
    :language: python
    :start-after: __write_csv_begin__
    :end-before: __write_csv_end__

.. tabbed:: JSON

  .. literalinclude:: ./doc_code/saving_datasets.py
    :language: python
    :start-after: __write_json_begin__
    :end-before: __write_json_end__

.. tabbed:: NumPy

  .. literalinclude:: ./doc_code/saving_datasets.py
    :language: python
    :start-after: __write_numpy_begin__
    :end-before: __write_numpy_end__

.. tabbed:: TFRecords

  .. literalinclude:: ./doc_code/saving_datasets.py
    :language: python
    :start-after: __write_tfrecords_begin__
    :end-before: __write_tfrecords_end__
