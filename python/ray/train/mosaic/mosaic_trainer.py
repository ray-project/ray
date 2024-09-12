from ray.util.annotations import Deprecated

_DEPRECATION_MESSAGE = (
    "`ray.train.mosaic.MosaicTrainer` is deprecated. "
    "Use `ray.train.torch.TorchTrainer` instead. "
    "See this issue for more information: "
    "https://github.com/ray-project/ray/issues/42893"
)


# TODO(justinvyu): [code_removal] Delete in Ray 2.11.
@Deprecated
class MosaicTrainer:
    """Deprecated. See this issue for more information:
    https://github.com/ray-project/ray/issues/42893
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
