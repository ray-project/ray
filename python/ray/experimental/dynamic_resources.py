def set_resource(resource_name, capacity, node_id=None):
    raise RuntimeError(
        "Dynamic custom resources have been removed. Consider using placement "
        "groups instead (docs.ray.io/en/master/placement-group.html). You "
        "can also specify resources at Ray start time with the 'resources' "
        "field in the cluster autoscaler."
    )
