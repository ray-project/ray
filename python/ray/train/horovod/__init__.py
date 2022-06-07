from ray.train.horovod.impl import HorovodConfig

# NEW API
from ray.train.horovod.horovod_trainer import HorovodTrainer

__all__ = ["HorovodConfig", "HorovodTrainer"]
