"""Pre-download an experiment's model + dataset into a shared HF cache.

Run once on the head node before launching a distributed run so the N training
workers hit a warm cache instead of each making cold, unauthenticated HF Hub
requests concurrently (slow, bursty, rate-limited). Also makes benchmark
timings reproducible by taking dataset download out of the measured loop.
"""

import logging
import os

from core.experiment_config import ExperimentConfig
from data.text_dataset import prepare_text_dataset, shared_hf_cache

logger = logging.getLogger(__name__)

# Adapters whose model.name is a HuggingFace repo id worth prefetching.
_HF_MODEL_ADAPTERS = {"deepspeed", "torchtitan", "megatron_bridge"}


def set_shared_hf_home() -> None:
    """Point HF_HOME at shared cluster storage if available and not already set."""
    cache = shared_hf_cache()
    if cache and not os.environ.get("HF_HOME"):
        os.environ["HF_HOME"] = cache
    logger.info(f"HF_HOME={os.environ.get('HF_HOME', '<default>')}")


def prepare_experiment(cfg: ExperimentConfig) -> None:
    """Warm the HF cache for this experiment's model and dataset."""
    set_shared_hf_home()

    if cfg.model.adapter in _HF_MODEL_ADAPTERS:
        from huggingface_hub import snapshot_download

        logger.info(f"Prefetching model '{cfg.model.name}' into HF cache.")
        snapshot_download(repo_id=cfg.model.name)
        logger.info(f"Model '{cfg.model.name}' is cached.")

    prepare_text_dataset(cfg.data.dataset)
