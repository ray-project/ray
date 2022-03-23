from ray.ml.train.data_parallel_trainer import DataParallelTrainer

from ray.train.horovod import HorovodConfig


class HorovodTrainer(DataParallelTrainer):
    def __init__(self, train_loop_per_worker, backend_config: HorovodConfig, **kwargs):
        super().__init__(train_loop_per_worker, backend_config=backend_config, **kwargs)
