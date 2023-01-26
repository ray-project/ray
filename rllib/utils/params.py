from ray.rllib.utils.annotations import ExperimentalAPI

@ExperimentalAPI
class Hyperparams(dict):
    """This is an extention of the dict class that allows access via `.` notation."""
    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value
