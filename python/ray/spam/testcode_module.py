def should_pass():
    """
    .. testcode::

        x = 3
"""


def should_fail():
    """
    .. testcode::

        print("ham")

    .. testoutput::

        eggs
"""


def check_state():
    """
    .. testcode::

        import ray
        assert not ray.is_initialized()
        ray.init()
"""
