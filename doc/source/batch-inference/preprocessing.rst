.. _inference_preprocessing:

Preprocessing Data
==================

After loading in a Dataset, the input the input data often needs to be preprocessed prior to inference. This may include cropping or resizing images, or tokenizing raw text.

With :ref:`Ray Data <datasets>`, you can define user-defined functions (UDFs) that transform batches of your data. Applying these UDFs via :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` will output a new, transformed Dataset.

.. tip::
    
    For the full suite of Dataset transformations, read :ref:`transforming_datasets`.


Writing batch UDFs
------------------
Mapping a UDF over batches is the simples transform for Ray Datasets.

The UDF defines the logic for transforming individual batches of data of the dataset Performing operations over batches of data is more performant than single element operations as it can leverage the underlying vectorization capabilities of Pandas or Numpy.

The following is an example to make use of those transformation APIs for processing
the Iris dataset.

.. literalinclude:: ../data/doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_transformation_begin__
   :end-before: __dataset_transformation_end__

Batch formats
~~~~~~~~~~~~~~
The batches that your UDF accepts can be different formats depending on the `batch_format` that is passed to :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`

Here is an overview of the available batch formats:

.. tabbed:: "default"

  The "default" batch format presents data as follows for each Dataset type:

  * **Tabular Datasets**: Each batch will be a
    `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
    This may incur a conversion cost if the underlying Dataset block is not
    zero-copy convertible from an Arrow table.

    .. literalinclude:: ../data/doc_code/transforming_datasets.py
      :language: python
      :start-after: __writing_default_udfs_tabular_begin__
      :end-before: __writing_default_udfs_tabular_end__

  * **Tensor Datasets** (single-column): Each batch will be a single
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
    containing the single tensor column for this batch.

    .. literalinclude:: ../data/doc_code/transforming_datasets.py
      :language: python
      :start-after: __writing_default_udfs_tensor_begin__
      :end-before: __writing_default_udfs_tensor_end__

  * **Simple Datasets**: Each batch will be a Python list.

    .. literalinclude:: ../data/doc_code/transforming_datasets.py
      :language: python
      :start-after: __writing_default_udfs_list_begin__
      :end-before: __writing_default_udfs_list_end__

.. tabbed:: "pandas"

  The ``"pandas"`` batch format presents batches in
  `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
  format. If converting a simple dataset to Pandas DataFrame batches, a single-column
  dataframe with the column ``"__value__"`` will be created.

  .. literalinclude:: ../data/doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_pandas_udfs_begin__
    :end-before: __writing_pandas_udfs_end__

.. tabbed:: "numpy"

  The ``"numpy"`` batch format presents batches in
  `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
  format as follows:

  * **Tabular Datasets**: Each batch will be a dictionary of NumPy
    ndarrays (``Dict[str, np.ndarray]``), with each key-value pair representing a column
    in the table.

  * **Tensor Datasets** (single-column): Each batch will be a single
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
    containing the single tensor column for this batch.

  * **Simple Datasets**: Each batch will be a single NumPy ndarray, where Datasets will
    attempt to convert each list-batch to an ndarray.

  .. literalinclude:: ../data/doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_udfs_begin__
    :end-before: __writing_numpy_udfs_end__


Configuring Batch Size
~~~~~~~~~~~~~~~~~~~~~~

An important parameter to set for :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` is ``batch_size``, which controls the size of the batches provided to the UDF.

.. literalinclude:: ../data/doc_code/transforming_datasets.py
  :language: python
  :start-after: __configuring_batch_size_begin__
  :end-before: __configuring_batch_size_end__

Increasing ``batch_size`` can result in faster execution by better leveraging vectorized
operations and hardware, reducing batch slicing and concatenation overhead, and overall
saturation of CPUs/GPUs, but will also result in higher memory utilization, which can
lead to out-of-memory failures. If encountering OOMs, decreasing your ``batch_size`` may
help.

.. note::
  The default ``batch_size`` of ``4096`` may be too large for datasets with large rows
  (e.g. tables with many columns or a collection of large images).