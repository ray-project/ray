from ray.util.annotations import Deprecated

_DEPRECATION_MESSAGE = (
    "`ray.train.sklearn.SklearnTrainer` is deprecated. "
    "Write your own training loop instead and use `ray.tune.Tuner` "
    "to parallelize the training of multiple sklearn models."
    "See this issue for a migration example: "
    "https://github.com/ray-project/ray/issues/42257"
)


# TODO(justinvyu): [code_removal] Delete in Ray 2.11.
@Deprecated
class SklearnTrainer:
    """Deprecated. See this issue for a migration example:
    https://github.com/ray-project/ray/issues/42257
    """

    def __new__(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)

    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)

    @classmethod
    def restore(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)

    @classmethod
    def can_restore(cls, *args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)

    @staticmethod
    def get_model(*args, **kwargs):
        raise DeprecationWarning(_DEPRECATION_MESSAGE)
