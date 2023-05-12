.. doctest::

    >>> import torch
    >>> torch.cuda.is_available()
    True

.. testcode::

    import ray
    assert not ray.is_initialized()
