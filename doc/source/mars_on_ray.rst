Mars on Ray
============

.. _`issue on GitHub`: https://github.com/mars-project/mars/issues


`Mars`_ is dedicated to leverage parallel and distributed technology to accelerate
numpy, pandas, scikit-learn and Python functions. Mars on Ray makes it easy to scala
your programs to a Ray cluster.


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

It's easy to run Mars job on Ray cluster. Use ``from mars.session import new_session``
and run ``new_session(backend='ray').as_default()``, this will create
a Mars session for Ray as default session, and then all Mars tasks will be
submitted to Ray cluster. Arguments will be passed to ``Ray.init()`` when
create Mars session by calling like
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

  Mars remote API is not available for now.

