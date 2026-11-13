from ray.train.constants import V1_REMOVAL_VERSION, V2_MIGRATION_GUIDE_MESSAGE
from ray.train.utils import _make_deprecated_module_getattr
from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class SessionMisuseError(Exception):
    """Indicates a method or function was used outside of a session."""


# `ray.train.error` is part of Ray Train v1. Ray code imports the private name
# below, and users get a deprecation warning through `__getattr__`.
_SessionMisuseError = SessionMisuseError
del SessionMisuseError

__getattr__ = _make_deprecated_module_getattr(
    __name__,
    {
        "SessionMisuseError": (
            _SessionMisuseError,
            "`ray.train.error.SessionMisuseError` is part of Ray Train v1, which is "
            f"deprecated and will be removed in {V1_REMOVAL_VERSION}. Ray Train v2 "
            "doesn't raise it. "
            f"{V2_MIGRATION_GUIDE_MESSAGE}",
        ),
    },
)
