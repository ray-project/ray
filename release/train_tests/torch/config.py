import enum

import pydantic


class DataloaderType(enum.Enum):
    RAY_DATA = "ray_data"
    MOCK = "mock"


class BenchmarkConfig(pydantic.BaseModel):
    # ScalingConfig
    num_workers: int = 1
    use_gpu: bool = True

    # Training
    num_epochs: int = 1

    # Model
    task: str = "image_classification"
    model_name: str = "resnet50"

    # Data
    dataset_name: str = "imagenet"
    dataloader_type: DataloaderType = DataloaderType.RAY_DATA

    # Validation
    validate_every_n_steps: int = 0
    validate_at_epoch_end: bool = True
