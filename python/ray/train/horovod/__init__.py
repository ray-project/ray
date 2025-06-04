# isort: off
try:
    import horovod  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Horovod isn't installed. To install Horovod with PyTorch support, run 'pip "
        "install 'horovod[pytorch]''. To install Horovod with TensorFlow support, "
        "run 'pip install 'horovod[tensorflow]''."
    )
# isort: on

from ray.train.horovod.config import HorovodConfig
from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.v2._internal.constants import is_v2_enabled

if is_v2_enabled():
    from ray.train.v2.horovod.horovod_trainer import HorovodTrainer  # noqa: F811

__all__ = ["HorovodConfig", "HorovodTrainer"]


# DO NOT ADD ANYTHING AFTER THIS LINE.
