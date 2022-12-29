.. _api-guide-for-users-from-other-data-libs:

API Guide for Users from Other Data Libraries
=============================================

Ray Datasets is a data loading and preprocessing library for ML. It shares certain
similarities with other ETL data processing libraries, but also has its own focus.
In this API guide, we will provide API mappings for users who come from those data
libraries, so you can quickly map what you may already know to Ray Datasets APIs.

.. note::

  - This is meant to map APIs that perform comparable but not necessarily identical operations.
    Please check the API reference for exact semantics and usage.
  - This list may not be exhaustive: Ray Datasets is not a traditional ETL data processing library, so not all data processing APIs can map to Datasets.
    In addition, we try to focus on common APIs or APIs that are less obvious to see a connection.

.. _api-guide-for-pandas-users:

For Pandas Users
----------------

.. list-table:: Pandas DataFrame vs. Ray Datasets APIs
   :header-rows: 1

   * - Pandas DataFrame API
     - Ray Datasets API
   * - df.head()
     - :meth:`ds.show() <ray.data.Dataset.show>` or :meth:`ds.take() <ray.data.Dataset.take>`
   * - df.dtypes
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
   * - len(df) or df.shape[0]
     - :meth:`ds.count() <ray.data.Dataset.count>`
   * - df.truncate()
     - :meth:`ds.limit() <ray.data.Dataset.limit>`
   * - df.iterrows()
     - :meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>`
   * - df.drop()
     - :meth:`ds.drop_columns() <ray.data.Dataset.drop_columns>`
   * - df.transform()
     - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` or :meth:`ds.map() <ray.data.Dataset.map>`
   * - df.groupby()
     - :meth:`ds.groupby() <ray.data.Dataset.groupby>`
   * - df.groupby().apply()
     - :meth:`ds.groupby().map_groups() <ray.data.grouped_dataset.GroupedDataset.map_groups>`
   * - df.sample()
     - :meth:`ds.random_sample() <ray.data.Dataset.random_sample>`
   * - df.sort_values()
     - :meth:`ds.sort() <ray.data.Dataset.sort>`
   * - df.append()
     - :meth:`ds.union() <ray.data.Dataset.union>`
   * - df.aggregate()
     - :meth:`ds.aggregate() <ray.data.Dataset.aggregate>`
   * - df.min()
     - :meth:`ds.min() <ray.data.Dataset.min>`
   * - df.max()
     - :meth:`ds.max() <ray.data.Dataset.max>`
   * - df.sum()
     - :meth:`ds.sum() <ray.data.Dataset.sum>`
   * - df.mean()
     - :meth:`ds.mean() <ray.data.Dataset.mean>`
   * - df.std()
     - :meth:`ds.std() <ray.data.Dataset.std>`

.. _api-guide-for-pyarrow-users:

For PyArrow Users
-----------------

.. list-table:: PyArrow Table vs. Ray Datasets APIs
   :header-rows: 1

   * - PyArrow Table API
     - Ray Datasets API
   * - pa.Table.schema
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
   * - pa.Table.num_rows
     - :meth:`ds.count() <ray.data.Dataset.count>`
   * - pa.Table.filter()
     - :meth:`ds.filter() <ray.data.Dataset.filter>`
   * - pa.Table.drop()
     - :meth:`ds.drop_columns() <ray.data.Dataset.drop_columns>`
   * - pa.Table.add_column()
     - :meth:`ds.add_column() <ray.data.Dataset.add_column>`
   * - pa.Table.groupby()
     - :meth:`ds.groupby() <ray.data.Dataset.groupby>`
   * - pa.Table.sort_by()
     - :meth:`ds.sort() <ray.data.Dataset.sort>`
