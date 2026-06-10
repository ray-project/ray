import sys

import pytest

from ray.rllib.env.env_runner_state_server import EnvRunnerStateServer
from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO


def _state(seq_no, weights="WEIGHTS_OBJ_REF"):
    # The server stores whatever StateDict it is handed. In production the `rl_module`
    # value is a `ray.ObjectRef`, but the server never dereferences it, so a plain
    # sentinel is sufficient to exercise the store/serve logic here.
    return {WEIGHTS_SEQ_NO: seq_no, "rl_module": weights}


def test_empty_server():
    server = EnvRunnerStateServer()
    assert server.pull() is None
    assert server.get_version() == -1


def test_push_pull_roundtrip_preserves_state_verbatim():
    server = EnvRunnerStateServer()
    state = _state(3)
    server.push(state)
    # Returned verbatim (including the - here sentinel - weights "ObjectRef"); the
    # server must NOT dereference or copy it.
    assert server.pull() is state
    assert server.pull()["rl_module"] == "WEIGHTS_OBJ_REF"
    assert server.get_version() == 3


def test_push_replaces_and_advances_version():
    server = EnvRunnerStateServer()
    server.push(_state(1))
    server.push(_state(2))
    assert server.get_version() == 2
    assert server.pull()[WEIGHTS_SEQ_NO] == 2


def test_push_rejects_state_without_seq_no():
    # A state without WEIGHTS_SEQ_NO could never be pulled (EnvRunners version-gate via
    # `pull_if_newer`), so the server rejects it loudly instead of holding state that no
    # EnvRunner would ever apply.
    server = EnvRunnerStateServer()
    with pytest.raises(ValueError, match="WEIGHTS_SEQ_NO"):
        server.push({"rl_module": "WEIGHTS_OBJ_REF"})
    # Nothing was stored.
    assert server.pull() is None
    assert server.get_version() == -1


def test_pull_if_newer_only_returns_strictly_newer_state():
    server = EnvRunnerStateServer()
    # Empty server -> nothing to return, regardless of the caller's version.
    assert server.pull_if_newer(-1) is None

    state = _state(5)
    server.push(state)
    # Equal-or-newer caller version -> None, so the (heavy) state is NOT transferred.
    assert server.pull_if_newer(5) is None
    assert server.pull_if_newer(6) is None
    # Older caller version -> the full state, returned verbatim (same object).
    assert server.pull_if_newer(4) is state
    assert server.pull_if_newer(-1) is state


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
