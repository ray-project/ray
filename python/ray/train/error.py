from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
class SessionMisuseError(Exception):
    """Indicates a method or function was used outside of a session."""
