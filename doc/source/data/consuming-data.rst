.. _consuming_data:

=====================
Consuming Data
=====================

The data underlying a ``Datastream`` can be consumed in several ways:

* Retrieving a limited prefix of rows.
* Iterating over rows and batches.
* Saving to files.

Retrieving a limited set of rows
================================

A limited set of rows can be retrieved from a ``Datastream`` via the
:meth:`ds.take() <ray.data.Datastream.take>` or :meth:`ds.take_batch() <ray.data.Datastream.take_batch>`
APIs, and :meth:`ds.show() <ray.data.Datastream.show>`, for printing a limited set of rows. These
methods are convenient for quickly inspecting a subset (prefix) of rows.

.. literalinclude:: ./doc_code/consuming_data.py
  :language: python
  :start-after: __take_begin__
  :end-before: __take_end__

Iterating over Datastreams
==========================

Datastreams can be consumed a row at a time using the
:meth:`ds.iter_rows() <ray.data.Datastream.iter_rows>` API

.. literalinclude:: ./doc_code/consuming_data.py
  :language: python
  :start-after: __iter_rows_begin__
  :end-before: __iter_rows_end__

or a batch at a time using the
:meth:`ds.iter_batches() <ray.data.Datastream.iter_batches>` API, where you can specify
batch size as well as the desired batch format. By default, the batches have type
``Dict[str, np.ndarray]`` (NumPy format).

.. literalinclude:: ./doc_code/consuming_data.py
  :language: python
  :start-after: __iter_batches_begin__
  :end-before: __iter_batches_end__

Datastreams can be passed to Ray tasks or actors and accessed by these iteration methods.
This does not incur a copy, since the blocks of the Datastream are passed by reference as Ray objects:

.. literalinclude:: ./doc_code/consuming_data.py
  :language: python
  :start-after: __remote_iterators_begin__
  :end-before: __remote_iterators_end__


Splitting Into and Consuming Shards
===================================

Datastreams can be split up into disjoint iterators, or shards.
This is a common pattern useful for loading and sharding data between distributed training actors:

.. note::

  If using :ref:`Ray Train <train-docs>` for distributed training, you do not need to split the datastream; Ray
  Train will automatically do locality-aware splitting into per-trainer shards for you.

.. literalinclude:: ./doc_code/consuming_data.py
  :language: python
  :start-after: __split_begin__
  :end-before: __split_end__

.. _saving_data:

Saving Data
==================

Datastreams can be written to local or remote storage in the desired data format.
The supported formats include Parquet, CSV, JSON, NumPy. To control the number
of output files, you may use :meth:`ds.repartition() <ray.data.Datastream.repartition>`
to repartition the Datastream before writing out.

.. tab-set::

    .. tab-item:: Parquet

      .. literalinclude:: ./doc_code/saving_data.py
        :language: python
        :start-after: __write_parquet_begin__
        :end-before: __write_parquet_end__

    .. tab-item:: CSV

      .. literalinclude:: ./doc_code/saving_data.py
        :language: python
        :start-after: __write_csv_begin__
        :end-before: __write_csv_end__

    .. tab-item:: JSON

      .. literalinclude:: ./doc_code/saving_data.py
        :language: python
        :start-after: __write_json_begin__
        :end-before: __write_json_end__

    .. tab-item:: NumPy

      .. literalinclude:: ./doc_code/saving_data.py
        :language: python
        :start-after: __write_numpy_begin__
        :end-before: __write_numpy_end__

    .. tab-item:: TFRecords

      .. literalinclude:: ./doc_code/saving_data.py
        :language: python
        :start-after: __write_tfrecords_begin__
        :end-before: __write_tfrecords_end__
