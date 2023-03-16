from typing import TYPE_CHECKING

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer


def tag_air_trainer(trainer: "BaseTrainer"):
    from ray.train.trainer import BaseTrainer
    from ray.train.registry import AIR_TRAINERS

    assert isinstance(trainer, BaseTrainer)
    module_path = trainer.__module__
    if module_path.startswith("ray.train"):
        trainer_name = "{trainer.__class__.__name__}"
        if trainer_name not in AIR_TRAINERS:
            trainer_name = "CustomTrainer"
    else:
        trainer_name = "CustomTrainer"
    record_extra_usage_tag(TagKey.AIR_TRAINER, trainer_name)
