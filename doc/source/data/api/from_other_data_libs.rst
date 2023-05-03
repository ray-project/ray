.. _api-guide-for-users-from-other-data-libs:

API Guide for Users from Other Data Libraries
=============================================

Ray Data is a data loading and preprocessing library for ML. It shares certain
similarities with other ETL data processing libraries, but also has its own focus.
In this API guide, we will provide API mappings for users who come from those data
libraries, so you can quickly map what you may already know to Ray Data APIs.

.. note::

  - This is meant to map APIs that perform comparable but not necessarily identical operations.
    Please check the API reference for exact semantics and usage.
  - This list may not be exhaustive: Ray Data is not a traditional ETL data processing library, so not all data processing APIs can map to Datastreams.
    In addition, we try to focus on common APIs or APIs that are less obvious to see a connection.

.. _api-guide-for-pandas-users:

For Pandas Users
----------------

.. list-table:: Pandas DataFrame vs. Ray Data APIs
   :header-rows: 1

   * - Pandas DataFrame API
     - Ray Data API
   * - df.head()
     - :meth:`ds.show() <ray.data.Datastream.show>`, :meth:`ds.take() <ray.data.Datastream.take>`, or :meth:`ds.take_batch() <ray.data.Datastream.take_batch>`
   * - df.dtypes
     - :meth:`ds.schema() <ray.data.Datastream.schema>`
   * - len(df) or df.shape[0]
     - :meth:`ds.count() <ray.data.Datastream.count>`
   * - df.truncate()
     - :meth:`ds.limit() <ray.data.Datastream.limit>`
   * - df.iterrows()
     - :meth:`ds.iter_rows() <ray.data.Datastream.iter_rows>`
   * - df.drop()
     - :meth:`ds.drop_columns() <ray.data.Datastream.drop_columns>`
   * - df.transform()
     - :meth:`ds.map_batches() <ray.data.Datastream.map_batches>` or :meth:`ds.map() <ray.data.Datastream.map>`
   * - df.groupby()
     - :meth:`ds.groupby() <ray.data.Datastream.groupby>`
   * - df.groupby().apply()
     - :meth:`ds.groupby().map_groups() <ray.data.grouped_data.GroupedData.map_groups>`
   * - df.sample()
     - :meth:`ds.random_sample() <ray.data.Datastream.random_sample>`
   * - df.sort_values()
     - :meth:`ds.sort() <ray.data.Datastream.sort>`
   * - df.append()
     - :meth:`ds.union() <ray.data.Datastream.union>`
   * - df.aggregate()
     - :meth:`ds.aggregate() <ray.data.Datastream.aggregate>`
   * - df.min()
     - :meth:`ds.min() <ray.data.Datastream.min>`
   * - df.max()
     - :meth:`ds.max() <ray.data.Datastream.max>`
   * - df.sum()
     - :meth:`ds.sum() <ray.data.Datastream.sum>`
   * - df.mean()
     - :meth:`ds.mean() <ray.data.Datastream.mean>`
   * - df.std()
     - :meth:`ds.std() <ray.data.Datastream.std>`

.. _api-guide-for-pyarrow-users:

For PyArrow Users
-----------------

.. list-table:: PyArrow Table vs. Ray Data APIs
   :header-rows: 1

   * - PyArrow Table API
     - Ray Data API
   * - pa.Table.schema
     - :meth:`ds.schema() <ray.data.Datastream.schema>`
   * - pa.Table.num_rows
     - :meth:`ds.count() <ray.data.Datastream.count>`
   * - pa.Table.filter()
     - :meth:`ds.filter() <ray.data.Datastream.filter>`
   * - pa.Table.drop()
     - :meth:`ds.drop_columns() <ray.data.Datastream.drop_columns>`
   * - pa.Table.add_column()
     - :meth:`ds.add_column() <ray.data.Datastream.add_column>`
   * - pa.Table.groupby()
     - :meth:`ds.groupby() <ray.data.Datastream.groupby>`
   * - pa.Table.sort_by()
     - :meth:`ds.sort() <ray.data.Datastream.sort>`
