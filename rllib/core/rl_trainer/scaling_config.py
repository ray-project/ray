from dataclasses import dataclass


@dataclass
class TrainerScalingConfig:
    """Configuratiom for scaling training actors.

    Attributes:
        num_workers: The number of workers to use for training. num_workers=0 means you
            have only one local worker (either on 1 CPU or 1 GPU)
        num_cpus_per_worker: The number of CPUs to allocate per worker. If
            num_workers=0 and num_gpus_per_worker=0, regardless of this value, the
            training will run on a single CPU.
        num_gpus_per_worker: The number of GPUs to allocate per worker. If
            num_workers=0, any number greater than 0 will run the training on a single
            GPU. A value of zero will run the training on a single CPU.
    """

    num_workers: int = 0
    num_cpus_per_worker: int = 1
    num_gpus_per_worker: int = 0
