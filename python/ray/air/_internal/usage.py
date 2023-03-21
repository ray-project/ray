from typing import TYPE_CHECKING

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer

AIR_TRAINERS = {
    "HorovodTrainer",
    "HuggingFaceTrainer",
    "LightGBMTrainer",
    "LightningTrainer",
    "MosaicTrainer",
    "RLTrainer",
    "SklearnTrainer",
    "TensorflowTrainer",
    "TorchTrainer",
    "XGBoostTrainer",
}


def tag_air_trainer(trainer: "BaseTrainer"):
    from ray.train.trainer import BaseTrainer

    assert isinstance(trainer, BaseTrainer)
    module_path = trainer.__module__
    if module_path.startswith("ray.train"):
        trainer_name = trainer.__class__.__name__
        if trainer_name not in AIR_TRAINERS:
            trainer_name = "Custom"
    else:
        trainer_name = "Custom"
    record_extra_usage_tag(TagKey.AIR_TRAINER, trainer_name)
