try:
    import horovod  # noqa: F401
except ModuleNotFoundError:
    raise ModuleNotFoundError(
        "Horovod isn't installed. To install Horovod with PyTorch support, run 'pip "
        "install 'horovod[pytorch]''. To install Horovod with TensorFlow support, "
        "run 'pip install 'horovod[tensorflow]''."
    )

from ray.train.horovod.horovod_trainer import HorovodTrainer
from ray.train.horovod.config import HorovodConfig

__all__ = ["HorovodConfig", "HorovodTrainer"]
