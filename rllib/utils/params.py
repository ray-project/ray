from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
class Hyperparams(dict):
    """This is an extention of the dict class that allows access via `.` notation."""

    def __getattr__(self, key):
        if key in self:
            return self[key]
        else:
            return super().__getattr__(key)
