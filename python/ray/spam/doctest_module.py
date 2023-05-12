def should_pass():
    """
    >>> x = 3
    """


def should_fail():
    """
    .. doctest::

        >>> "spam"
        'bacon'
    """


def check_state():
    """
    >>> import ray
    >>> assert not ray.is_initialized()
    >>> ray.init()  # doctest: +ELLIPSIS
    RayContext(...)
    """
