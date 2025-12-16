from dataclasses import dataclass

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class DeploymentID:
    name: str
    app_name: str = "default"

    def to_replica_actor_class_name(self):
        return f"ServeReplica:{self.app_name}:{self.name}"

    def __str__(self):
        return f"Deployment(name='{self.name}', app='{self.app_name}')"

    def __repr__(self):
        return str(self)
