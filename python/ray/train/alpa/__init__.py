from typing import TYPE_CHECKING

if TYPE_CHECKING:
    try:
        import alpa  # noqa: F401
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            "Alpa isn't installed. To install Alpa, Refer to the document:"
            "https://alpa-projects.github.io/install.html#install-from-source"
        )

from ray.train.alpa.alpa_trainer import AlpaTrainer

__all__ = ["AlpaTrainer"]
