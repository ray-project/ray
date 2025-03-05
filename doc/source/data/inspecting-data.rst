.. _inspecting-data:

===============
Inspecting Data
===============

Inspect :class:`Datasets <ray.data.Dataset>` to better understand your data.

This guide shows you how to:

* `Describe datasets <#describing-datasets>`_
* `Inspect rows <#inspecting-rows>`_
* `Inspect batches <#inspecting-batches>`_
* `Inspect execution statistics <#inspecting-execution-statistics>`_

.. _describing-datasets:

Describing datasets
===================

:class:`Datasets <ray.data.Dataset>` are tabular. To view a dataset's column names and
types, call :meth:`Dataset.schema() <ray.data.Dataset.schema>`.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    print(ds.schema())

.. testoutput::

    Column             Type
    ------             ----
    sepal length (cm)  double
    sepal width (cm)   double
    petal length (cm)  double
    petal width (cm)   double
    target             int64

For more information like the number of rows, print the Dataset.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    print(ds)

.. testoutput::

    Dataset(
       num_rows=150,
       schema={
          sepal length (cm): double,
          sepal width (cm): double,
          petal length (cm): double,
          petal width (cm): double,
          target: int64
       }
    )

.. _inspecting-rows:

Inspecting rows
===============

To get a list of rows, call :meth:`Dataset.take() <ray.data.Dataset.take>` or
:meth:`Dataset.take_all() <ray.data.Dataset.take_all>`. Ray Data represents each row as
a dictionary.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    rows = ds.take(1)
    print(rows)

.. testoutput::

    [{'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}]


For more information on working with rows, see
:ref:`Transforming rows <transforming_rows>` and
:ref:`Iterating over rows <iterating-over-rows>`.

.. _inspecting-batches:

Inspecting batches
==================

A batch contains data from multiple rows. To inspect batches, call
`Dataset.take_batch() <ray.data.Dataset.take_batch>`.

By default, Ray Data represents batches as dicts of NumPy ndarrays. To change the type
of the returned batch, set ``batch_format``.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            batch = ds.take_batch(batch_size=2, batch_format="numpy")
            print("Batch:", batch)
            print("Image shape", batch["image"].shape)

        .. testoutput::
            :options: +MOCK

            Batch: {'image': array([[[[...]]]], dtype=uint8)}
            Image shape: (2, 32, 32, 3)

    .. tab-item:: pandas

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            batch = ds.take_batch(batch_size=2, batch_format="pandas")
            print(batch)

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

               sepal length (cm)  sepal width (cm)  ...  petal width (cm)  target
            0                5.1               3.5  ...               0.2       0
            1                4.9               3.0  ...               0.2       0
            <BLANKLINE>
            [2 rows x 5 columns]

For more information on working with batches, see
:ref:`Transforming batches <transforming_batches>` and
:ref:`Iterating over batches <iterating-over-batches>`.


Inspecting execution statistics
===============================

Ray Data calculates statistics during execution for each operator, such as wall clock time and memory usage.

To view stats about your :class:`Datasets <ray.data.Dataset>`, call :meth:`Dataset.stats() <ray.data.Dataset.stats>` on an executed dataset. The stats are also persisted under `/tmp/ray/session_*/logs/ray-data/ray-data.log`.
For more on how to read this output, see :ref:`Monitoring Your Workload with the Ray Data Dashboard <monitoring-your-workload>`.

.. testcode::

    import ray
    import datasets

    def f(batch):
        return batch

    def g(row):
        return True

    hf_ds = datasets.load_dataset("mnist", "mnist")
    ds = (
        ray.data.from_huggingface(hf_ds["train"])
        .map_batches(f)
        .filter(g)
        .materialize()
    )

    print(ds.stats())

.. testoutput::
    :options: +MOCK

    Operator 1 ReadParquet->SplitBlocks(32): 1 tasks executed, 32 blocks produced in 2.92s
    * Remote wall time: 103.38us min, 1.34s max, 42.14ms mean, 1.35s total
    * Remote cpu time: 102.0us min, 164.66ms max, 5.37ms mean, 171.72ms total
    * UDF time: 0us min, 0us max, 0.0us mean, 0us total
    * Peak heap memory usage (MiB): 266375.0 min, 281875.0 max, 274491 mean
    * Output num rows per block: 1875 min, 1875 max, 1875 mean, 60000 total
    * Output size bytes per block: 537986 min, 555360 max, 545963 mean, 17470820 total
    * Output rows per task: 60000 min, 60000 max, 60000 mean, 1 tasks used
    * Tasks per node: 1 min, 1 max, 1 mean; 1 nodes used
    * Operator throughput:
        * Ray Data throughput: 20579.80984833993 rows/s
        * Estimated single node throughput: 44492.67361278733 rows/s

    Operator 2 MapBatches(f)->Filter(g): 32 tasks executed, 32 blocks produced in 3.63s
    * Remote wall time: 675.48ms min, 1.0s max, 797.07ms mean, 25.51s total
    * Remote cpu time: 673.41ms min, 897.32ms max, 768.09ms mean, 24.58s total
    * UDF time: 661.65ms min, 978.04ms max, 778.13ms mean, 24.9s total
    * Peak heap memory usage (MiB): 152281.25 min, 286796.88 max, 164231 mean
    * Output num rows per block: 1875 min, 1875 max, 1875 mean, 60000 total
    * Output size bytes per block: 530251 min, 547625 max, 538228 mean, 17223300 total
    * Output rows per task: 1875 min, 1875 max, 1875 mean, 32 tasks used
    * Tasks per node: 32 min, 32 max, 32 mean; 1 nodes used
    * Operator throughput:
        * Ray Data throughput: 16512.364546087643 rows/s
        * Estimated single node throughput: 2352.3683708977856 rows/s

    Dataset throughput:
        * Ray Data throughput: 11463.372316361854 rows/s
        * Estimated single node throughput: 25580.963670075285 rows/s
