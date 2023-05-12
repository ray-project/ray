.. _mars-on-ray:

Using Mars on Ray
=================

.. _`issue on GitHub`: https://github.com/mars-project/mars/issues


`Mars`_ is a tensor-based unified framework for large-scale data computation which scales Numpy, Pandas and Scikit-learn.
Mars on Ray makes it easy to scale your programs with a Ray cluster. Currently Mars on Ray supports both Ray actors 
and tasks as execution backend. The task will be scheduled by mars scheduler if Ray actors is used. This mode can reuse 
all mars scheduler optimizations. If ray tasks mode is used, all tasks will be scheduled by ray, which can reuse failover and
pipeline capabilities provided by ray futures.


.. _`Mars`: https://mars-project.readthedocs.io/en/latest/


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


Interact with Datastream:


.. code-block:: python

    import mars.tensor as mt
    import mars.dataframe as md
    df = md.DataFrame(
        mt.random.rand(1000_0000, 4),
        columns=list('abcd'))
    # Convert mars dataframe to ray datastream
    import ray
    # ds = md.to_ray_dataset(df)
    ds = ray.data.from_mars(df)
    print(ds.schema(), ds.count())
    ds.filter(lambda row: row["a"] > 0.5).show(5)
    # Convert ray datastream to mars dataframe
    # df2 = md.read_ray_dataset(ds)
    df2 = ds.to_mars()
    print(df2.head(5).execute())

Refer to _`Mars on Ray`: https://mars-project.readthedocs.io/en/latest/installation/ray.html#mars-ray for more information.
