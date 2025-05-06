# TODO (genesu): revisit these data structures
from pydantic import ValidationError as PydanticValidationError
from abc import ABC, abstractmethod


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


class PromptTooLongError(ValidationError):
    pass


class TooManyStoppingSequencesError(ValidationError):
    pass


class ErrorReason(ABC):
    @abstractmethod
    def get_message(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.get_message()

    @property
    @abstractmethod
    def exception(self) -> Exception:
        raise NotImplementedError

    def raise_exception(self) -> Exception:
        raise self.exception


class InputTooLong(ErrorReason):
    def __init__(self, num_tokens: int, max_num_tokens: int) -> None:
        self.num_tokens = num_tokens
        self.max_num_tokens = max_num_tokens

    def get_message(self) -> str:
        if self.num_tokens < 0:
            return f"Input too long. The maximum input length is {self.max_num_tokens} tokens."
        return f"Input too long. Recieved {self.num_tokens} tokens, but the maximum input length is {self.max_num_tokens} tokens."

    @property
    def exception(self) -> Exception:
        return PromptTooLongError(self.get_message())


class TooManyStoppingSequences(ErrorReason):
    def __init__(
        self, num_stopping_sequences: int, max_num_stopping_sequences: int
    ) -> None:
        self.num_stopping_sequences = num_stopping_sequences
        self.max_num_stopping_sequences = max_num_stopping_sequences

    def get_message(self) -> str:
        return (
            f"Too many stopping sequences. Recieved {self.num_stopping_sequences} stopping sequences,"
            f"but the maximum is {self.max_num_stopping_sequences}. Please reduce the number of provided stopping sequences."
        )

    @property
    def exception(self) -> Exception:
        return TooManyStoppingSequencesError(self.get_message())
