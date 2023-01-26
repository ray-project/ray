import pytest
from typing import Dict, Optional

from ray.tune.callback import Callback, CallbackList


class StatefulCallback(Callback):
    CKPT_FILE_TMPL = "test-callback-state-{}.json"

    def __init__(self):
        self.counter = 0

    def on_trial_result(self, iteration, trials, trial, result, **info):
        self.counter += 1

    def get_state(self) -> Optional[Dict]:
        return {"counter": self.counter}

    def set_state(self, state: Dict):
        self.counter = state["counter"]


def test_stateful_callback_save_and_restore(tmp_path):
    """Checks that a stateful callback can be saved to a directory and restored with
    the right state."""

    callback = StatefulCallback()
    for i in range(3):
        callback.on_trial_result(i, None, None, None)
    callback.save_to_dir(str(tmp_path))
    assert list(tmp_path.glob(StatefulCallback.CKPT_FILE_TMPL.format("*")))
    assert callback.can_restore(str(tmp_path))

    restored_callback = StatefulCallback()
    restored_callback.restore_from_dir(str(tmp_path))
    assert restored_callback.counter == 3


def test_stateless_callback_save_and_restore(tmp_path):
    """Checks that proper errors are raised/handled when saving/restoring a
    stateless callback (i.e. one that doesn't implement get/set_state)."""

    class StatelessCallback(Callback):
        def handle_save_error(self, error: Exception):
            assert isinstance(error, NotImplementedError)

    callback = StatelessCallback()
    callback.save_to_dir(str(tmp_path))

    assert not list(tmp_path.glob(StatelessCallback.CKPT_FILE_TMPL.format("*")))
    assert not callback.can_restore(str(tmp_path))
    with pytest.raises(RuntimeError):
        callback.restore_from_dir(str(tmp_path))


def test_callback_list_with_stateful_callback(tmp_path):
    """Checks that a callback list saves and restores all callbacks contained
    inside it."""

    callbacks = CallbackList([Callback(), StatefulCallback()])
    for i in range(3):
        callbacks.on_trial_result(iteration=i, trials=None, trial=None, result=None)

    callbacks.save_to_dir(str(tmp_path))

    assert list(tmp_path.glob(CallbackList.CKPT_FILE_TMPL.format("*")))
    assert callbacks.can_restore(str(tmp_path))

    restored_callbacks = CallbackList([Callback(), StatefulCallback()])
    restored_callbacks.restore_from_dir(str(tmp_path))

    assert restored_callbacks._callbacks[1].counter == 3


def test_callback_list_without_stateful_callback(tmp_path):
    """If no callbacks within a CallbackList are stateful, then nothing
    should be saved."""

    callbacks = CallbackList([Callback(), Callback()])
    callbacks.save_to_dir(str(tmp_path))

    assert not list(tmp_path.glob(CallbackList.CKPT_FILE_TMPL.format("*")))
    assert not callbacks.can_restore(str(tmp_path))

    with pytest.raises(RuntimeError):
        callbacks.restore_from_dir(str(tmp_path))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
