Ray Datasets
============

.. warning::
  This feature is currently in preview, and its API may change.

Ray Datasets are the standard way to load and exchange data between Ray libraries and applications. You can think of a Ray Dataset as a distributed collection of data records, which can be either serializable Python objects or structured data (Arrow records). Datasets support basic parallel transformations, and can be ``split()`` into pieces for ingest into Ray's distributed machine learning libraries.

For those familiar with Apache Arrow, Ray Datasets can be thought of as implementing "distributed Arrow".

Under the hood, Datasets are implemented using Ray objects. Data records are organized into data *blocks*, each of which is a Ray object holding an Arrow table. A Dataset is simply a list of these block references and their metadata. Datasets implement parallel operations using Ray tasks and actors to transform their data blocks. Since Datasets are just lists of Ray object refs, they can also be passed between Ray tasks and actors just like any other object. Datasets support conversion to/from several more featureful dataframe libraries (e.g., Spark, Dask, Modin, MARS), and also conversion into TensorFlow and PyTorch dataset formats.

Creating, Transforming, and Saving Datasets
-------------------------------------------

talk about ray.data.read_*, from_<df>, read_datasource, range, items for testing

Example: ray.data.range(100) -> str(), count, schema, etc. inspection

Example: read parquet, basic transform, saving data

Consuming datasets
------------------

talk about split(), iter_batch/rows, to_torch(), to_tf(), to_<df>

Example: passing datasets between Ray tasks / actors

Example: pseudocode read features, split, send to training actors

Example: converting to/from a dask-on-ray dataset

Parallel transforms
-------------------

talk about map fn, repartition

Example: read binary files, map_batches with actors / GPU

Example: map_batches using pandas batch udf for efficiency

Pipelining data processing and ML computations
----------------------------------------------

feature coming soon

Custom datasources
------------------

talk about custom datasource examples and contributions welcome
