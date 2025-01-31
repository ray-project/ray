import enum

import pydantic


class DataloaderType(enum.Enum):
    RAY_DATA = "ray_data"
    MOCK = "mock"


class BenchmarkConfig(pydantic.BaseModel):
    # ScalingConfig
    num_workers: int = 1

    # Model
    task: str = "image_classification"
    model_name: str = "resnet50"

    # Data
    dataloader_type: DataloaderType = DataloaderType.RAY_DATA

    # Training
    num_epochs: int = 1
    skip_train_step: bool = False
    train_batch_size: int = 32

    # Validation
    validate_every_n_steps: int = -1
    skip_validation_at_epoch_end: bool = False
    validation_batch_size: int = 256

    # Logging
    log_metrics_every_n_steps: int = 0
