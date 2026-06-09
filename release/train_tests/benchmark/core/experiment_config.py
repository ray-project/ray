"""Centralized experiment definition for Ray Train benchmarks.

An *Experiment* corresponds to one benchmark run: a model + framework adapter,
a dataset, a training schedule, and an optional perturbation, all defined in a
single YAML file under ``experiments/``. Adding a new benchmark case should
only require a new YAML file unless a new framework adapter is needed.

Example:
    cfg = load_experiment("experiments/qwen3_06b_deepspeed.yaml")
"""

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ModelConfig:
    # Adapter that knows how to train this model. Dispatched by core.runner:
    # "deepspeed" | "torchtitan" | "maxtext" | "image_classification" | "recsys"
    adapter: str
    # Model identifier. HF model id for LLM adapters (e.g. "Qwen/Qwen3-0.6B"),
    # torchvision model name for image_classification (e.g. "resnet50").
    name: str
    # Adapter-specific parallelism spec, e.g. {"zero_stage": 3} for deepspeed
    # or {"dp_shard": -1, "tp": 2, "pp": 2} for torchtitan.
    parallelism: Dict[str, Any] = field(default_factory=dict)
    # "bf16" | "fp16" | "fp32". Adapters may downgrade (e.g. bf16 -> fp16 on T4).
    precision: str = "bf16"
    # Trade recompute for activation memory. Essential to fit large
    # batch x seq on memory-constrained GPUs (e.g. 24GB A10G) and for big models.
    gradient_checkpointing: bool = False
    # HF attention kernel: "sdpa" (memory-efficient, default) | "eager"
    # (materializes the full B x H x S x S score matrix — OOMs at long seq) |
    # "flash_attention_2" (needs the flash-attn package).
    attn_implementation: str = "sdpa"
    # Escape hatch for framework-native config (path to ds_config.json,
    # torchtitan .toml, or inline kwargs). Merged on top of what the adapter
    # derives from this config.
    framework_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataConfig:
    # "ray_data" | "torch" | "framework_default"
    dataloader: str = "framework_default"
    # Registered dataset name: "c4" | "wikitext" | "ag_news" | "synthetic"
    # | "imagenet_parquet" | "criteo" | "fashion_mnist"
    dataset: str = "synthetic"
    # S3 root, HF dataset id, or empty for built-in datasets.
    dataset_path: str = ""
    # Per-device micro batch size.
    batch_size: int = 1
    # Sequence length for LLM workloads; None for non-token workloads.
    seq_len: Optional[int] = None
    # Cap on training rows to read. -1 means unlimited.
    limit_training_rows: int = -1


@dataclass
class TrainingConfig:
    # Exactly one of num_steps / num_epochs should be set.
    num_steps: Optional[int] = None
    num_epochs: Optional[int] = None
    seed: int = 42
    # Steps excluded from steady-state throughput / MFU metrics.
    warmup_steps: int = 5
    log_every_n_steps: int = 10
    optimizer: Dict[str, Any] = field(
        default_factory=lambda: {
            "lr": 3.0e-4,
            "betas": [0.9, 0.95],
            "weight_decay": 0.1,
        }
    )
    # Gradient accumulation steps (micro-batches per optimizer step).
    gradient_accumulation_steps: int = 1


@dataclass
class CheckpointConfig:
    # Checkpoint every N optimizer steps. -1 disables mid-epoch checkpointing.
    interval: int = -1
    # Checkpoint at the end of the run, to exercise the save path at least once.
    at_end: bool = False


@dataclass
class PerturbationConfig:
    # "preemption" | "chaos_kill" | "slow_consumer" | "network_failure"
    kind: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentConfig:
    name: str
    model: ModelConfig
    data: DataConfig = field(default_factory=DataConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    perturbation: Optional[PerturbationConfig] = None
    # "ray" | "torchrun" (torchrun is the parity baseline).
    launcher: str = "ray"
    num_workers: int = 1
    use_gpu: bool = True
    max_failures: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def _build_dataclass(cls, data: Dict[str, Any]):
    """Construct a dataclass from a dict, erroring on unknown keys."""
    if data is None:
        data = {}
    field_names = {f.name for f in dataclasses.fields(cls)}
    unknown = set(data) - field_names
    if unknown:
        raise ValueError(
            f"Unknown keys {sorted(unknown)} for {cls.__name__}. "
            f"Valid keys: {sorted(field_names)}"
        )
    return cls(**data)


def _apply_overrides(raw: Dict[str, Any], overrides: List[str]) -> Dict[str, Any]:
    """Apply 'a.b.c=value' style CLI overrides onto the raw YAML dict."""
    for override in overrides:
        if "=" not in override:
            raise ValueError(f"Override must look like key.path=value, got: {override}")
        key_path, value = override.split("=", 1)
        node = raw
        keys = key_path.split(".")
        for key in keys[:-1]:
            node = node.setdefault(key, {})
        node[keys[-1]] = yaml.safe_load(value)
    return raw


def experiment_from_dict(raw: Dict[str, Any]) -> ExperimentConfig:
    raw = dict(raw)
    model = _build_dataclass(ModelConfig, raw.pop("model", None))
    data = _build_dataclass(DataConfig, raw.pop("data", None))
    training = _build_dataclass(TrainingConfig, raw.pop("training", None))
    checkpoint = _build_dataclass(CheckpointConfig, raw.pop("checkpoint", None))

    perturbation_raw = raw.pop("perturbation", None)
    perturbation = (
        _build_dataclass(PerturbationConfig, perturbation_raw)
        if perturbation_raw
        else None
    )

    cfg = ExperimentConfig(
        model=model,
        data=data,
        training=training,
        checkpoint=checkpoint,
        perturbation=perturbation,
        **raw,
    )

    if cfg.training.num_steps is None and cfg.training.num_epochs is None:
        raise ValueError("One of training.num_steps / training.num_epochs must be set.")
    return cfg


def load_experiment(
    path: str, overrides: Optional[List[str]] = None
) -> ExperimentConfig:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    if overrides:
        raw = _apply_overrides(raw, overrides)
    return experiment_from_dict(raw)
