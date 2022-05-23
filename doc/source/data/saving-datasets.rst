.. _saving_datasets:

===============
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
