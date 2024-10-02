How to One-hot Encode Data
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::
    :hide:

    assert False

.. CVGA-start 
One-hot encoding is a technique used to convert categorical data into a numerical format 
that ML algorithms can understand. This tutorial shows you how to use this technique 
with Ray Data. 
.. CVGA-end

Load the data
-------------

Call a function like :func:`ray.data.read_parquet` to load data into a Ray Dataset.

.. testcode:: 

    import ray
    
    ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

Here's what this dataset looks like before one-hot encoding:

.. testcode::

    print(ds.schema())

.. testoutput::

    Column              Type                                                                                
    ------              ----                                                                                  
    sepal.length        double                                                           
    sepal.width         double                                                                                                                                                                 
    petal.length        double
    petal.width         double
    variety             string

Determine the unique values
---------------------------

Call :meth:`ray.data.Dataset.unique` to determine the unique values in the column you 
want to one-hot encode.

.. testcode::

    unique_varieties = ds.unique("variety")

Here are the unique values in the "variety" column:

.. testcode::
    :hide:

    # Sort the unique values to prevent non-deteministic output.
    unique_varieties.sort()

.. testcode::

    print(unique_varieties)

.. testoutput::

    ['setosa', 'versicolor', 'virginica']


One-hot encode the data
-----------------------

Define a function that takes a batch of data and one-hot encodes the "variety" column. 

.. testcode::

    import pandas as pd
    
    def one_hot_encode(batch: pd.DataFrame) -> pd.DataFrame:
        # 'get_dummies' in a function that efficiently one-hot encodes a column.
        return pd.get_dummies(batch, columns=["variety"])

Then, call :meth:`ray.data.Dataset.map_batches` to one-hot encode the "variety" column.

.. testcode::

    one_hot_encoded = ds.map_batches(one_hot_encode, batch_format="pandas")

Here's what the dataset looks like after one-hot encoding:

.. testcode::

    print(one_hot_encoded.schema())

.. testoutput::

    Column              Type                                                                                
    ------              ----                                                                                  
    sepal.length        double                                                           
    sepal.width         double                                                                                                                                                                 
    petal.length        double
    petal.width         double
    variety_Setosa      uint8
    variety_Versicolor  uint8
    variety_Virginica   uint8
