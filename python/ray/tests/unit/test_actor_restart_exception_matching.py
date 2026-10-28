import sys

import pytest

from ray._common.ray_option_utils import (
    validate_actor_options,
    validate_task_options,
)


@pytest.fixture
def match():
    from ray._raylet import _match_restart_exception

    return _match_restart_exception


def test_validate_restart_on_exceptions_accepts_valid_values():
    validate_actor_options({"restart_on_exceptions": None}, in_options=True)
    validate_actor_options(
        {"restart_on_exceptions": ["CUDA error", "out of memory"]},
        in_options=True,
    )


def test_validate_restart_on_exceptions_rejects_invalid_values():
    with pytest.raises(TypeError):
        validate_actor_options(
            {"restart_on_exceptions": ("CUDA error",)}, in_options=True
        )
    with pytest.raises(ValueError):
        validate_actor_options({"restart_on_exceptions": ["ok", 123]}, in_options=True)
    with pytest.raises(ValueError):
        validate_actor_options({"restart_on_exceptions": ["   "]}, in_options=True)


def test_restart_on_exceptions_is_rejected_for_tasks():
    with pytest.raises(ValueError, match="restart_on_exceptions"):
        validate_task_options(
            {"restart_on_exceptions": ["CUDA error"]}, in_options=True
        )


def test_matcher_matches_case_insensitive_substring(match):
    exc = RuntimeError("CUDA Error: Out Of Memory")
    assert match(exc, []) is None
    assert match(exc, ["disk full"]) is None
    assert match(exc, ["nope", "out of memory"]) == "out of memory"


def test_matcher_walks_cause_chain(match):
    try:
        try:
            raise ValueError("inner cuda failure")
        except ValueError as inner:
            raise RuntimeError("outer wrapper") from inner
    except RuntimeError as outer:
        exc = outer
    assert match(exc, ["inner cuda failure"]) == "inner cuda failure"


@pytest.mark.parametrize("raises_on_bool", [False, True])
def test_matcher_does_not_evaluate_cause_truthiness(match, raises_on_bool):
    class CauseError(Exception):
        def __bool__(self):
            if raises_on_bool:
                raise RuntimeError("exception truthiness must not be evaluated")
            return False

    exc = RuntimeError("outer wrapper")
    exc.__cause__ = CauseError("CUDA fault")
    exc.__context__ = ValueError("unrelated context")
    assert match(exc, ["CUDA fault"]) == "CUDA fault"
    assert match(exc, ["unrelated context"]) is None


def test_matcher_walks_suppressed_context_chain(match):
    try:
        try:
            raise ValueError("cuda failure")
        except ValueError:
            # `from None` suppresses the context for display, but the matcher
            # still walks it so wrapped faults are caught.
            raise RuntimeError("outer wrapper") from None
    except RuntimeError as outer:
        exc = outer
    assert match(exc, ["cuda failure"]) == "cuda failure"


def test_matcher_terminates_on_exception_cycles(match):
    exc = RuntimeError("cycle")
    exc.__cause__ = exc
    assert match(exc, ["cycle"]) == "cycle"


@pytest.mark.parametrize("str_error", [RuntimeError, SystemExit])
def test_safe_restart_formatter(match, str_error):
    from ray._raylet import _format_restart_exception

    class BrokenStringError(Exception):
        def __str__(self):
            raise str_error("formatting failed")

    exc = BrokenStringError()
    assert _format_restart_exception(exc) == (
        "BrokenStringError: <exception str() failed>"
    )
    assert match(exc, ["brokenstringerror"]) == "brokenstringerror"
    exc.__cause__ = ValueError("inner CUDA fault")
    assert match(exc, ["inner cuda"]) == "inner cuda"
    assert match(exc, ["unrelated"]) is None
    assert _format_restart_exception(ValueError("ordinary")) == "ValueError: ordinary"


def test_restart_exit_preserves_cause():
    from ray._raylet import _ActorRestartRequested, _exit_for_actor_restart

    original = ValueError("original fault")
    request = _ActorRestartRequested("restart reason")
    request.__cause__ = original
    with pytest.raises(SystemExit) as caught:
        _exit_for_actor_restart(request)
    assert caught.value.code == 1
    assert caught.value.ray_terminate_msg == "restart reason"
    assert caught.value.__cause__ is request
    assert caught.value.__cause__.__cause__ is original


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
