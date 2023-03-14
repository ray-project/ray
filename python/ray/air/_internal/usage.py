from typing import TYPE_CHECKING

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

if TYPE_CHECKING:
    from ray.train.trainer import BaseTrainer


def tag_air_trainer(trainer: "BaseTrainer"):
    from ray.train.trainer import BaseTrainer

    assert isinstance(trainer, BaseTrainer)
    module_path = trainer.__module__
    if module_path.startswith("ray.train"):
        result = f"{trainer.__class__.__name__}"
    else:
        result = "custom_trainer"
    import ipdb; ipdb.set_trace()
    record_extra_usage_tag(TagKey.AIR_TRAINER, result)
