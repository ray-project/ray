from typing import Dict, Optional

import pytest

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
