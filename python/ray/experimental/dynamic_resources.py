def set_resource(resource_name, capacity, node_id=None):
    raise DeprecationWarning(
        "Dynamic custom resources are deprecated. Consider using placement "
        "groups instead (docs.ray.io/en/master/placement-group.html). You "
        "can also specify resources at Ray start time with the 'resources' "
        "field in the cluster autoscaler.")
