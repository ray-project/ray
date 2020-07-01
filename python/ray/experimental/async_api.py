import ray


def as_future(object_id):
    """Turn an object_id into a Future object.

    Args:
        object_id: A Ray object_id.

    Returns:
        Future: A future object that waits the object_id.
    """
    return object_id.as_future()
