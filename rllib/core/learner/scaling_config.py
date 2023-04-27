from dataclasses import dataclass


@dataclass
class LearnerGroupScalingConfig:
    """Configuratiom for scaling training actors.

    Attributes:
        num_workers: The number of workers to use for training. num_workers=0 means you
            have only one local worker (either on 1 CPU or 1 GPU)
        num_cpus_per_worker: The number of CPUs to allocate per worker. If
            num_workers=0 and num_gpus_per_worker=0, regardless of this value, the
            training will run on a single CPU.
        num_gpus_per_worker: The number of GPUs to allocate per worker. If
            num_workers=0, any number greater than 0 will run the training on a single
            GPU. A value of zero will run the training on `num_cpus_per_worker` CPUs.
            Fractional values (e.g. 0.5) are currently NOT supported as these might
            cause CUDA async errors.
        local_gpu_idx: if num_gpus_per_worker > 0, and num_workers<2, then this gpu
            index will be used for training. This is an index into the available cuda
            devices. For example if os.environ["CUDA_VISIBLE_DEVICES"] = "1" then a
            local_gpu_idx of 0 will use the gpu with id 1 on the node.
    """

    num_workers: int = 0
    num_cpus_per_worker: int = 1
    num_gpus_per_worker: int = 0
    local_gpu_idx: int = 0
