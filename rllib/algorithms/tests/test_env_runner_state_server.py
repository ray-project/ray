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


def test_get_version_without_seq_no():
    # A state lacking WEIGHTS_SEQ_NO reports -1 (treated as "no usable version"), so an
    # EnvRunner comparing `version > self._weights_seq_no` will never apply it.
    server = EnvRunnerStateServer()
    server.push({"rl_module": "WEIGHTS_OBJ_REF"})
    assert server.get_version() == -1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
