.. _api-guide-for-users-from-other-data-libs:

API Guide for Users from Other Data Libraries
=============================================

Ray Data is a data loading and preprocessing library for ML. It shares certain
similarities with other ETL data processing libraries, but also has its own focus.
This guide provides API mappings for users who come from those data
libraries, so you can quickly map what you may already know to Ray Data APIs.

.. note::

  - This is meant to map APIs that perform comparable but not necessarily identical operations.
    Select the API reference for exact semantics and usage.
  - This list may not be exhaustive: It focuses on common APIs or APIs that are less obvious to see a connection.

.. _api-guide-for-pandas-users:

For Pandas Users
----------------

.. list-table:: Pandas DataFrame vs. Ray Data APIs
   :header-rows: 1

   * - Pandas DataFrame API
     - Ray Data API
   * - df.head()
     - :meth:`ds.show() <ray.data.Dataset.show>`, :meth:`ds.take() <ray.data.Dataset.take>`, or :meth:`ds.take_batch() <ray.data.Dataset.take_batch>`
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
     - :meth:`ds.groupby().map_groups() <ray.data.grouped_data.GroupedData.map_groups>`
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

.. list-table:: PyArrow Table vs. Ray Data APIs
   :header-rows: 1

   * - PyArrow Table API
     - Ray Data API
   * - ``pa.Table.schema``
     - :meth:`ds.schema() <ray.data.Dataset.schema>`
   * - ``pa.Table.num_rows``
     - :meth:`ds.count() <ray.data.Dataset.count>`
   * - ``pa.Table.filter()``
     - :meth:`ds.filter() <ray.data.Dataset.filter>`
   * - ``pa.Table.drop()``
     - :meth:`ds.drop_columns() <ray.data.Dataset.drop_columns>`
   * - ``pa.Table.add_column()``
     - :meth:`ds.with_column() <ray.data.Dataset.with_column>`
   * - ``pa.Table.groupby()``
     - :meth:`ds.groupby() <ray.data.Dataset.groupby>`
   * - ``pa.Table.sort_by()``
     - :meth:`ds.sort() <ray.data.Dataset.sort>`


For PyTorch Dataset & DataLoader Users
--------------------------------------

For more details, see the :ref:`Migrating from PyTorch to Ray Data <migrate_pytorch>`.
