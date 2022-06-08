.. _accessing_datasets:

===================
Accessing Datasets
===================

The data underlying a ``Dataset`` can be accessed in several ways:

* Retrieving a limited prefix of rows.
* Iterating over rows and batches.
* Converting into a Torch dataset or a TensorFlow dataset.
* Converting into a RandomAccessDataset for random access (experimental).

Retrieving limited set of rows
==============================

A limited set of rows can be retried from a ``Dataset`` via the
:meth:`ds.take() <ray.data.Dataset.take>` API, along with its sibling helper APIs
:meth:`ds.take_all() <ray.data.Dataset.take_all>`, for retrieving **all** rows, and
:meth:`ds.show() <ray.data.Dataset.show>`, for printing a limited set of rows. These
methods are convenient for quickly inspecting a subset (prefix) of rows. They have the
benefit that, if used right after reading, they will only trigger more files to be
read if needed to retrieve rows from that file; if inspecting a small prefix of rows,
often only the first file will need to be read.

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __take_begin__
  :end-before: __take_end__

Iterating over Datasets
=======================

Datasets can be consumed a row at a time using the
:meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>` API

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __iter_rows_begin__
  :end-before: __iter_rows_end__

or a batch at a time using the
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` API, where you can specify
batch size as well as the desired batch format. By default, the batch format is
``"native"``, which means that the batch format that's native to the data type will be
returned. For tabular data, the native format is a Pandas DataFrame; for Python objects,
it's a list.

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __iter_batches_begin__
  :end-before: __iter_batches_end__


Datasets can be passed to Ray tasks or actors and accessed by these iteration methods.
This does not incur a copy, since the blocks of the Dataset are passed by reference as Ray objects:

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __remote_iterators_begin__
  :end-before: __remote_iterators_end__

Converting to Torch dataset
===========================

For ingestion into one or more Torch trainers, Datasets offers a :meth:`ds.to_torch()
<ray.data.Dataset.to_torch>` API that returns a
`Torch IterableDataset <https://pytorch.org/docs/stable/data.html#torch.utils.data.IterableDataset>`__
that the Torch trainers can consume. This API takes care of both batching and converting
the underlying Datasets data to Torch tensors, building on top of the
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` API.

.. note::

  The returned ``torch.utils.data.IterableDataset`` instance should be consumed directly
  in your training loop directly; it should **not** be used with the Torch data loader.
  Using Torch's data loader isn't necessary because upstream Ray Datasets preprocessing
  operations in conjunction with :meth:`ds.to_torch() <ray.data.Dataset.to_torch>`
  implements the data loader functionality (shuffling, batching, prefetching, etc.). If
  you use the Torch data loader with this ``IterableDataset``, it will perform
  inefficient unbatching and rebatching without adding any value. 

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __torch_begin__
  :end-before: __torch_end__

When performing supervised learning, we'll have both feature columns and a label column
that we may want to split into separate tensors. By informing ``ds.to_torch()`` of the
label column, it will yield ``(features, label)`` tensor pairs for each batch.

.. note::

  We set ``unsqueeze_label_tensor=False`` in order to remove a redundant unit column
  dimension. E.g., with ``batch_size=2`` and ``unsqueeze_label_tensor=True``, you would
  get ``(2, 1)``-shaped label tensor batches instead of the desired ``(2,)`` shape.

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __torch_with_label_begin__
  :end-before: __torch_with_label_end__

The types of the label and feature columns will be inferred from the data by default;
these can be overridden with the ``label_column_dtype`` and ``feature_column_dtypes``
args.

By default, all feature columns will be concatenated into a single tensor; however,
depending on the structure of the ``feature_columns`` argument, you can also get feature
column batches as a list of tensors or a dict of tensors (with one or more column in
each tensor). See the :meth:`.to_torch() API docs <ray.data.Dataset.to_torch>` for
details.

.. note::

  If we have tensor feature columns (where each item in the column is an multi-dimensional
  tensor) and any of the feature columns are different shapes, these columns are
  incompatible and we will not be able to stack the column tensors into a single tensor.
  Instead, we will need to group the columns by compatibility in the ``feature_columns``
  argument.

  Check out the :ref:`tensor data feature guide <datasets_tensor_ml_exchange>` for more
  information on how to handle this.

Converting to TensorFlow dataset
================================

For ingestion into one or more TensorFlow trainers, Datasets offers a :meth:`ds.to_tf()
<ray.data.Dataset.to_tf>` API that returns a
`tf.data.Dataset <https://www.tensorflow.org/api_docs/python/tf/data/Dataset>`__
that the TensorFlow trainers can consume. This API takes care of both batching and converting
the underlying Datasets data to TensorFlow tensors, building on top of the
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>` API.

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __tf_begin__
  :end-before: __tf_end__

When performing supervised learning, we'll have both feature columns and a label column
that we may want to split into separate tensors. By informing ``ds.to_tf()`` of the
label column, it will yield ``(features, label)`` tensor pairs for each batch.

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __tf_with_label_begin__
  :end-before: __tf_with_label_end__

The types of the label and feature columns will be inferred from the data by default;
these can be overridden with the ``label_column_dtype`` and ``feature_column_dtypes``
args.

By default, all feature columns will be concatenated into a single tensor; however,
depending on the structure of the ``feature_columns`` argument, you can also get feature
column batches as a list of tensors or a dict of tensors (with one or more column in
each tensor). See the :meth:`.to_tf() API docs <ray.data.Dataset.to_tf>` for
details.

.. note::

  If we have tensor feature columns (where each item in the column is an multi-dimensional
  tensor) and any of the feature columns are different shapes, these columns are
  incompatible and we will not be able to stack the column tensors into a single tensor.
  Instead, we will need to group the columns by compatibility in the ``feature_columns``
  argument.

  Check out the :ref:`tensor data feature guide <datasets_tensor_ml_exchange>` for more
  information on how to handle this.

Splitting Into and Consuming Shards
===================================

Datasets can be split up into disjoint sub-datasets, or shards.
Locality-aware splitting is supported if you pass in a list of actor handles to the
:meth:`ds.split() <ray.data.Dataset.split>` function along with the number of desired splits.
This is a common pattern useful for loading and sharding data between distributed training actors:

.. note::

  If using :ref:`Ray Train <train-docs>` for distributed training, you do not need to split the dataset; Ray
  Train will automatically do locality-aware splitting into per-trainer shards for you!

.. literalinclude:: ./doc_code/accessing_datasets.py
  :language: python
  :start-after: __split_begin__
  :end-before: __split_end__

Random Access Datasets (Experimental)
=====================================

Datasets can be converted to a format that supports efficient random access with
:meth:`ds.to_random_access_dataset() API <ray.data.Dataset.to_random_access_dataset>`,
which partitions the dataset on a sort key and provides random access via distributed
binary search.

See the :ref:`random access feature guide <datasets_random_access>` for more
information.
