import ray


def set_resource(resource_name, capacity, client_id=None):
    """ Set a resource to a specified capacity.

    This creates, updates or deletes a custom resource for a target clientId.
    If the resource already exists, it's capacity is updated to the new value.
    If the capacity is set to 0, the resource is deleted.
    If ClientID is not specified or set to None,
    the resource is created on the local client where the actor is running.

    Args:
        resource_name (str): Name of the resource to be created
        capacity (int): Capacity of the new resource. Resource is deleted if
            capacity is 0.
        client_id (str): The ClientId of the node where the resource is to be
            set.

    Returns:
        None

    Raises:
          ValueError: This exception is raised when a non-negative capacity is
            specified.
    """
    if client_id is not None:
        client_id_obj = ray.ClientID(ray.utils.hex_to_binary(client_id))
    else:
        client_id_obj = ray.ClientID.nil()
    if (capacity < 0) or (capacity != int(capacity)):
        raise ValueError(
            "Capacity {} must be a non-negative integer.".format(capacity))
    return ray.worker.global_worker.core_worker.set_resource(
        resource_name, capacity, client_id_obj)
