``cloudpickle-generators``
=========================

`cloudpickle <https://github.com/cloudpipe/cloudpickle>`_ support for
generators, including partially consumed generators.

Usage
-----

To use ``cloudpickle-generators``, you must simply call ``register``. After
calling register, you may use ``cloudpickle`` like normal.

.. code-block:: python

   >>> import cloudpickle
   >>> import cloudpickle_generators

   # register the generator support
   >>> cloudpickle_generators.register()

   # define a simple generator function that has both intermediate state (the
   # loop) and arguments
   >>> def f(a, b):
   ...     for n in range(a):
   ...         yield b + b[-1] * n

   >>> gen = f(4, 'ay')

   # advance the generator half way though before serializing
   >>> next(gen)
   ay
   >>> next(gen)
   ayy

   # do a serialization round trip
   >>> new_gen = cloudpickle.loads(cloudpickle.dumps(gen))
   >>> new_gen is gen
   False

   # advance the newly created generator instance
   >>> next(new_gen)
   ayyy
   >>> next(new_gen)
   ayyyy
   >>> next(new_gen)
   Traceback (most recent call last):
      ...
   StopIteration

   # the old instance is unaffected by the new one!
   >>> next(gen)
   ayyy
   >>> next(gen)
   ayyyy
   >>> next(gen)
   Traceback (most recent call last):
      ...
   StopIteration
