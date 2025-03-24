.. _data_quickstart:

Ray Data basics
===============

This guide introduces you to the core capabilities of Ray Data and includes examples of using the Ray :class:`Dataset <ray.data.Dataset>` abstraction for the following tasks:

* :ref:`Loading data <loading_key_concept>`
* :ref:`Transforming data <transforming_key_concept>`
* :ref:`Consuming data <consuming_key_concept>`
* :ref:`Saving data <saving_key_concept>`

The code examples provided in this guide build upon one another sequentially. You can run these commands interactively in a Jupyter notebook or on Anyscale. 

Provided code example materializes results to preview data at each step. When moving code to production, only include operations that materialize results if they are essential to your application. This allows Ray to optimize the logical and physical plans. See :ref:`How does Ray Data plan and execute operations?<plans>`



Combine these steps to complete data preprocessing or ETL (extract, transform, and load) workloads. For an example of 



What is a Ray Dataset?
----------------------

Ray Data's main abstraction is a :class:`Dataset <ray.data.Dataset>`, which represents a distributed collection of data. Datasets are specifically designed for machine learning workloads and can efficiently handle data collections that exceed a single machine's memory.

Ray Datasets are similar to DataFrames and Datasets in TensorFlow, PyTorch, pandas, and Apache Spark. Ray Data provides interoperatibility with these and other libraries. See :ref:`Loading data from other libraries<load_data_libraries>`.

For details on how Ray Datasets represent data for distributed processing, see :ref:`How does Ray Data work?<data_key_concept>`

.. _loading_key_concept:

Load data
---------

You can load data to create datasets from sources including local files, Python objects, and cloud storage services like S3 or GCS.

Ray Data seamlessly integrates with any `filesystem supported by Arrow<http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__.

The following code example loads CSV data from an S3 bucket and previews the data:

.. testcode::

    import ray

    # Load a CSV dataset directly from S3
    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    
    # Preview the first record
    ds.show(limit=1)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}

To learn more about creating datasets from different sources, read :ref:`Loading data <loading_data>`.

.. _transforming_key_concept:

Transform data
--------------

Define data transformations 

Apply user-defined functions (UDFs) to transform datasets. Ray automatically parallelizes these transformations
across your cluster for better performance.

The following code example applies a UDF to calculate the petal area for the CSV data loaded

.. testcode::

    from typing import Dict
    import numpy as np

    # Define a transformation to compute a "petal area" attribute
    def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        vec_a = batch["petal length (cm)"]
        vec_b = batch["petal width (cm)"]
        batch["petal area (cm^2)"] = vec_a * vec_b
        return batch

    # Apply the transformation to our dataset
    transformed_ds = ds.map_batches(transform_batch)
    
    # View the updated schema with the new column
    # .materialize() will execute all the lazy transformations and
    # materialize the dataset into object store memory
    print(transformed_ds.materialize())

.. testoutput::

    MaterializedDataset(
       num_blocks=...,
       num_rows=150,
       schema={
          sepal length (cm): double,
          sepal width (cm): double,
          petal length (cm): double,
          petal width (cm): double,
          target: int64,
          petal area (cm^2): double
       }
    )

To explore more transformation capabilities, read :ref:`Transforming data <transforming_data>`.

.. _consuming_key_concept:

Consume data
------------

Access dataset contents through convenient methods like :meth:`~ray.data.Dataset.take_batch` and 
:meth:`~ray.data.Dataset.iter_batches`. You can also pass datasets directly to Ray Tasks or Actors
for distributed processing.

.. testcode::

    # Extract the first 3 rows as a batch for processing
    print(transformed_ds.take_batch(batch_size=3))

.. testoutput::
    :options: +NORMALIZE_WHITESPACE

    {'sepal length (cm)': array([5.1, 4.9, 4.7]),
        'sepal width (cm)': array([3.5, 3. , 3.2]),
        'petal length (cm)': array([1.4, 1.4, 1.3]),
        'petal width (cm)': array([0.2, 0.2, 0.2]),
        'target': array([0, 0, 0]),
        'petal area (cm^2)': array([0.28, 0.28, 0.26])}

For more details on working with dataset contents, see
:ref:`Iterating over Data <iterating-over-data>` and :ref:`Saving Data <saving-data>`.

.. _saving_key_concept:

Save data
---------

Export processed datasets to a variety of formats and storage locations using methods
like :meth:`~ray.data.Dataset.write_parquet`, :meth:`~ray.data.Dataset.write_csv`, and more.

.. testcode::
    :hide:

    # The number of blocks can be non-determinstic. Repartition the dataset beforehand
    # so that the number of written files is consistent.
    transformed_ds = transformed_ds.repartition(2)

.. testcode::

    import os

    # Save the transformed dataset as Parquet files
    transformed_ds.write_parquet("/tmp/iris")

    # Verify the files were created
    print(os.listdir("/tmp/iris"))

.. testoutput::
    :options: +MOCK

    ['..._000000.parquet', '..._000001.parquet']


For more information on saving datasets, see :ref:`Saving data <saving-data>`.


.. _etl_example:

ETL with Ray Data
-----------------

The following code example loads CSV data from S3, applies a data transformation to calculate a new field, and saves results using Parquet. 



.. testcode::
    :hide:

    # The number of blocks can be non-determinstic. Repartition the dataset beforehand
    # so that the number of written files is consistent.
    transformed_ds = transformed_ds.repartition(2)

.. testcode::

    import os

    # Save the transformed dataset as Parquet files
    transformed_ds.write_parquet("/tmp/iris")

    # Verify the files were created
    print(os.listdir("/tmp/iris"))

.. testoutput::
    :options: +MOCK

    ['..._000000.parquet', '..._000001.parquet']

.. _preprocessing_example:

Data preprocessing with Ray Data
--------------------------------

The following code example loads CSV data from S3, applies a data transformation to calculate a new field, and returns the result as the Python variable `train_dataset`.



