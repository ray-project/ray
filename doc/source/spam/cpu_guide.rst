.. testcode::

    print("ham")

.. testoutput::

    ham

.. doctest::

    >>> import torch
    >>> torch.cuda.is_available()
    False

.. testcode::

    import ray
    assert not ray.is_initialized()
