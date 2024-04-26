from .autoscaler import Autoscaler, DefaultAutoscaler


def create_autoscaler(topology, resource_manager, execution_id):
    return DefaultAutoscaler(topology, resource_manager, execution_id)


__all__ = [
    "Autoscaler",
    "DefaultAutoscaler",
    "create_autoscaler",
]
