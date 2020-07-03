from ray._raylet import (
    RayError,
    RayConnectionError,
    RayCancellationError,
    RayTaskError,
    RayWorkerError,
    RayActorError,
    RayletError,
        class cls(RayTaskError, cause_cls):
            def __getattr__(self, name):
                         proctitle, pid, ip):
                return getattr(cause, name)
                                      cause_cls, proctitle, pid, ip)
        name = "RayTaskError({})".format(cause_cls.__name__)
        return cls()
                   self.proctitle, self.pid, self.ip)
    ObjectStoreFullError,
    UnreconstructableError,
    RayTimeoutError,
    PlasmaObjectNotAvailable,
)

RAY_EXCEPTION_TYPES = [
    RayError,
    RayConnectionError,
    RayCancellationError,
    RayTaskError,
    RayWorkerError,
    RayActorError,
    RayletError,
    ObjectStoreFullError,
    UnreconstructableError,
    RayTimeoutError,
    PlasmaObjectNotAvailable,
]
