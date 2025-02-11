import argparse
import enum

from pydantic import BaseModel, Field


class DataloaderType(enum.Enum):
    RAY_DATA = "ray_data"
    MOCK = "mock"


class DataLoaderConfig(BaseModel):
    train_batch_size: int = 32
    validation_batch_size: int = 256


class RayDataConfig(DataLoaderConfig):
    # NOTE: Optional[int] doesn't play well with argparse.
    local_buffer_shuffle_size: int = -1


class BenchmarkConfig(BaseModel):
    # ScalingConfig
    num_workers: int = 1

    # Model
    task: str = "image_classification"

    # Data
    dataloader_type: DataloaderType = DataloaderType.RAY_DATA
    dataloader_config: DataLoaderConfig = Field(
        default_factory=lambda: DataLoaderConfig(),
    )

    # Training
    num_epochs: int = 1
    skip_train_step: bool = False

    # Validation
    validate_every_n_steps: int = -1
    skip_validation_at_epoch_end: bool = False

    # Logging
    log_metrics_every_n_steps: int = 512


def _is_pydantic_model(field_type) -> bool:
    """Check if a type is a subclass of Pydantic's BaseModel."""
    return isinstance(field_type, type) and issubclass(field_type, BaseModel)


def _add_field_to_parser(parser: argparse.ArgumentParser, field: str, field_info):
    field_type = field_info.annotation
    if field_type is bool:
        assert (
            not field_info.default
        ), "Only supports bool flags that are False by default."
        parser.add_argument(
            f"--{field}", action="store_true", default=field_info.default
        )
    else:
        parser.add_argument(f"--{field}", type=field_type, default=field_info.default)


def cli_to_config() -> BenchmarkConfig:
    parser = argparse.ArgumentParser()

    nested_fields = []
    for field, field_info in BenchmarkConfig.model_fields.items():
        # Skip nested configs for now
        if _is_pydantic_model(field_info.annotation):
            nested_fields.append(field)
            continue

        _add_field_to_parser(parser, field, field_info)

    top_level_args, _ = parser.parse_known_args()

    # Handle nested configs that depend on top-level args
    nested_configs = {}
    for nested_field in nested_fields:
        nested_parser = argparse.ArgumentParser()
        config_cls = BenchmarkConfig.model_fields[nested_field].annotation

        if (
            config_cls == DataLoaderConfig
            and top_level_args.dataloader_type == DataloaderType.RAY_DATA
        ):
            config_cls = RayDataConfig

        for field, field_info in config_cls.model_fields.items():
            _add_field_to_parser(nested_parser, field, field_info)

        args, _ = nested_parser.parse_known_args()
        nested_configs[nested_field] = config_cls(**vars(args))

    return BenchmarkConfig(**vars(top_level_args), **nested_configs)
