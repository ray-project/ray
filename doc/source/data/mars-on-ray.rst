.. _mars-on-ray:

Using Mars on Ray
=================

.. _`issue on GitHub`: https://github.com/mars-project/mars/issues


`Mars`_ is a tensor-based unified framework for large-scale data computation which scales Numpy, Pandas and Scikit-learn.
Mars on Ray makes it easy to scale your programs with a Ray cluster.

.. note::

  This API is experimental in Mars. If you encounter any bugs, please file an `issue on GitHub`_.


.. _`Mars`: https://docs.pymars.org


Installation
-------------
You can simply install Mars via pip:

.. code-block:: bash

    pip install pymars>=0.6.0a1


Getting started
----------------

It's easy to run Mars jobs on a Ray cluster. Use ``from mars.session import new_session``
and run ``new_session(backend='ray').as_default()``; this will create
a Mars session for Ray as the default session, and then all Mars tasks will be
submitted to Ray. Arguments will be passed to ``ray.init()`` when
creating a Mars session like this:
``new_session(backend='ray', address=<address>, num_cpus=<num_cpus>)``.


.. code-block:: python

    from mars.session import new_session
    ray_session = new_session(backend='ray').as_default()

    import mars.dataframe as md
    import mars.tensor as mt

    t = mt.random.rand(100, 4, chunk_size=30)
    df = md.DataFrame(t, columns=list('abcd'))
    print(df.describe().execute())


.. warning::

  `Mars remote API`_ is not available for now.


.. _`Mars remote API`: https://docs.pymars.org/en/latest/getting_started/remote.html
