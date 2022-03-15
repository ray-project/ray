from ray.tune import Callback


class TrialResultObserver(Callback):
    """Helper class to control runner.step() count."""

    def __init__(self):
        self._counter = 0
        self._last_counter = 0

    def reset(self):
        self._last_counter = self._counter

    def just_received_a_result(self):
        if self._last_counter == self._counter:
            return False
        else:
            self._last_counter = self._counter
            return True

    def on_trial_result(self, **kwargs):
        self._counter += 1
