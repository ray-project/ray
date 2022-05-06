import unittest


class TestTrialExecutorInheritance(unittest.TestCase):
    def test_direct_inheritance_not_ok(self):

        from ray.tune.trial_executor import TrialExecutor

        msg = (
            "_MyTrialExecutor inherits from TrialExecutor, which is being "
            "deprecated. "
            "RFC: https://github.com/ray-project/ray/issues/17593. "
            "Please reach out on the Ray Github if you have any concerns."
        )

        with self.assertRaisesRegex(DeprecationWarning, msg):

            class _MyTrialExecutor(TrialExecutor):
                def __init__(self):
                    pass

                def start_trial(self, trial):
                    return True

                def stop_trial(self, trial):
                    pass

                def restore(self, trial):
                    pass

                def save(self, trial):
                    return None

                def reset_trial(self, trial, new_config, new_experiment_tag):
                    return False

                def debug_string(self):
                    return "This is a debug string."

                def export_trial_if_needed(self):
                    return {}

                def fetch_result(self):
                    return []

                def get_next_available_trial(self):
                    return None

                def get_running_trials(self):
                    return []

    def test_indirect_inheritance_ok(self):
        from ray.tune.ray_trial_executor import RayTrialExecutor

        class _MyRayTrialExecutor(RayTrialExecutor):
            pass

        class _AnotherMyRayTrialExecutor(_MyRayTrialExecutor):
            pass
