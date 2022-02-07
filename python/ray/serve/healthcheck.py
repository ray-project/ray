from typing import Callable, Optional

HEALTHCHECK_DECORATOR_ATTRIBUTE = "__ray_serve_healthcheck_method"


def healthcheck(func: Callable) -> Callable:
    """Decorator for users to mark the method to use for health checking.

    This simply sets a well-known attribute on the method object, which we
    later search for when instantiating the replica.
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
