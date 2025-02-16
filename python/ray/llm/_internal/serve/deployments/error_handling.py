from pydantic import ValidationError as PydanticValidationError


class ValidationError(ValueError):
    status_code = 400
    pass


class ValidationErrorWithPydantic(ValidationError):
    """Wraps a PydanticValidationError to be used as a ValidationError.

    This is necessary as pydantic.ValidationError cannot be subclassed,
    which causes errors when Ray tries to wrap it in a
    RayTaskError/RayActorError."""

    def __init__(self, exc: PydanticValidationError) -> None:
        self.exc = exc
        # BaseException implements a __reduce__ method that returns
        # a tuple with the type and the value of self.args.
        # https://stackoverflow.com/a/49715949/2213289
        self.args = (exc,)

    def __getattr__(self, name):
        return getattr(self.exc, name)

    def __repr__(self) -> str:
        return self.exc.__repr__()

    def __str__(self) -> str:
        return self.exc.__str__()
