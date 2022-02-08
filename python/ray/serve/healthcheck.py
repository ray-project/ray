from typing import Callable, Optional

HEALTHCHECK_DECORATOR_ATTRIBUTE = "__ray_serve_healthcheck_method"


def healthcheck(func: Callable) -> Callable:
    """Decorator to define an application-level health check for a deployment.

    EXPERIMENTAL

    This should be used on a method of a Deployment class. The method should
    run to completion and return `None` if the replica is healthy, and raise
    an exception if the replica is unhealthy. Only one method per class may
    be decorated with this method. If none are defined, the only health check
    will be verifying that the replica is alive.

    The health check period and timeout can be configured in the deployment
    options.
    """
    setattr(func, HEALTHCHECK_DECORATOR_ATTRIBUTE, True)
    return func


def get_healthcheck_method(cls: type) -> Optional[str]:
    """Return the name of the user-defined health check method (if any).

    Raises an exception if the decorator was used on multiple methods.
    """
    healthcheck_method: Optional[str] = None
    for attr in dir(cls):
        if HEALTHCHECK_DECORATOR_ATTRIBUTE in dir(getattr(cls, attr)):
            if healthcheck_method is not None:
                raise ValueError(
                    f"@serve.healthcheck can only be defined on one method, "
                    f"but it was defined on both '{healthcheck_method}' and "
                    f"'{attr}'."
                )
            else:
                healthcheck_method = attr

    return healthcheck_method
