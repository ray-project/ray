.. _mars-on-ray:

Using Mars on Ray
=================

.. _`issue on GitHub`: https://github.com/mars-project/mars/issues


`Mars`_ is a tensor-based unified framework for large-scale data computation which scales Numpy, Pandas and Scikit-learn.
Mars on Ray makes it easy to scale your programs with a Ray cluster. Currently Mars on Ray uses Ray actors as execution
backend, Ray remote function based execution backend is working in progress.

.. image:: https://user-images.githubusercontent.com/12445254/159614968-26f67871-2569-4539-a0be-771c72ad2d1b.png
   :width: 650px
   :align: center


Installation
-------------
You can simply install Mars via pip:

.. code-block:: bash

    pip install pymars>=0.8.3


Getting started
----------------

It's easy to run Mars jobs on a Ray cluster.


Starting a new Mars on Ray runtime locally via:


.. code-block:: python

    import ray
    ray.init()
    import mars
    mars.new_ray_session()
    import mars.tensor as mt
    mt.random.RandomState(0).rand(1000_0000, 5).sum().execute()


Or connecting to a Mars on Ray runtime which is already initialized:


.. code-block:: python

    import mars
    mars.new_ray_session('http://<web_ip>:<ui_port>')
    # perform computation


Interact with Ray Dataset:


.. code-block:: python

    import mars.tensor as mt
    import mars.dataframe as md
    df = md.DataFrame(
        mt.random.rand(1000_0000, 4),
        columns=list('abcd'))
    # Convert mars dataframe to ray dataset
    import ray.data
    # ds = md.to_ray_dataset(df)
    ds = ray.data.from_mars(df)
    print(ds.schema(), ds.count())
    ds.filter(lambda row: row["a"] > 0.5).show(5)
    # Convert ray dataset to mars dataframe
    # df2 = md.read_ray_dataset(ds)
    df2 = ds.to_mars()
    print(df2.head(5).execute())

Refer to _`Mars on Ray`: https://docs.pymars.org/en/latest/installation/ray.htmlfor more information.
