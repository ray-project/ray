"""Centralized experiment definition for Ray Train benchmarks.

An *Experiment* corresponds to one benchmark run: a framework adapter, a model,
a dataset, a training schedule, and optional checkpoint/validation/perturbation,
all defined in a single YAML under ``experiments/``. Adding a new benchmark case
should only require a new YAML unless a new framework adapter is needed.

Example:
    cfg = load_experiment("experiments/qwen3_06b_deepspeed.yaml")
"""

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ModelConfig:
    # Model identifier. HF model id for LLM adapters (e.g. "Qwen/Qwen3.5-0.8B"),
    # torchvision model name for image_classification (e.g. "resnet50").
    name: str
    # Adapter-specific parallelism spec, e.g. {"zero_stage": 1} for deepspeed
    # or {"dp_shard": -1, "tp": 2, "pp": 2} for torchtitan.
    parallelism: Dict[str, Any] = field(default_factory=dict)
    # "bf16" | "fp16" | "fp32". Adapters may downgrade (e.g. bf16 -> fp16 on T4).
    precision: str = "bf16"
    # Trade recompute for activation memory (fit large batch x seq / big models).
    gradient_checkpointing: bool = False
    # HF attention kernel: "sdpa" (HF default for supported models, memory
    # efficient) | "eager" (materializes B*H*S*S scores) | "flash_attention_2".
    attn_implementation: str = "sdpa"
    # Escape hatch for framework-native config (ds_config.json path, torchtitan
    # .toml, inline kwargs). Merged on top of what the adapter derives.
    framework_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScalingConfig:
    """Cluster shape — mirrors ray.train.ScalingConfig so it maps 1:1."""

    num_workers: int = 1
    use_gpu: bool = True
    # Extra per-worker resources (e.g. {"CPU": 8}); None = Ray defaults.
    resources_per_worker: Optional[Dict[str, float]] = None
    # Pin workers to a GPU type (e.g. "A10G", "A100"); None = any.
    accelerator_type: Optional[str] = None


@dataclass
class DataConfig:
    # "native" (the adapter builds its own loader) | "ray_data" | "torch".
    dataloader: str = "native"
    # Registered dataset name: "c4" | "wikitext" | "ag_news" | "synthetic" | ...
    dataset: str = "synthetic"
    dataset_path: str = ""
    # Per-device micro-batch size (= train_micro_batch_size_per_gpu / MBS). The
    # global batch is micro_batch_size x data_parallel_size x grad_accum_steps.
    micro_batch_size: int = 1
    # Sequence length for LLM workloads; None for non-token workloads. Samples
    # are truncated/padded to this length (see text_dataset; packing is a TODO).
    seq_len: Optional[int] = None
    # Cap on training rows to read. -1 means unlimited.
    limit_training_rows: int = -1


@dataclass
class TrainingConfig:
    # Exactly one of num_steps / num_epochs should be set.
    num_steps: Optional[int] = None
    num_epochs: Optional[int] = None
    # Target global batch size. If set, grad-accum steps are derived as
    # global_batch_size / (micro_batch_size x data_parallel_size); if None,
    # grad-accum is 1 and the global batch is micro_batch_size x dp.
    global_batch_size: Optional[int] = None
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


@dataclass
class CheckpointConfig:
    # Checkpoint every N optimizer steps. -1 disables mid-run checkpointing.
    every_n_steps: int = -1
    # "sync" (blocking) | "async" (Ray Train async checkpointing — integration
    # pending; the slot lets experiments declare intent now).
    mode: str = "sync"
    # Checkpoint once at the end, to exercise the save path at least once.
    at_end: bool = False


@dataclass
class ValidationConfig:
    enabled: bool = False
    # Validate every N optimizer steps. -1 disables mid-run validation.
    every_n_steps: int = -1
    # "sync" | "async" (Ray Train async validation — integration pending).
    mode: str = "sync"


@dataclass
class PerturbationConfig:
    # "preemption" | "chaos_kill" | "slow_consumer" | "network_failure"
    kind: str = ""
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentConfig:
    name: str
    # Framework / training-path selector. Dispatched by core.registry:
    # "deepspeed" | "torchtitan" | "maxtext" | "image_classification" | ...
    adapter: str
    model: ModelConfig
    data: DataConfig = field(default_factory=DataConfig)
    training: TrainingConfig = field(default_factory=TrainingConfig)
    scaling: ScalingConfig = field(default_factory=ScalingConfig)
    checkpoint: CheckpointConfig = field(default_factory=CheckpointConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    perturbation: Optional[PerturbationConfig] = None
    # "ray_train" (Ray Train) | "torchrun_ray" (vanilla torch.distributed placed
    # by Ray actors — the parity baseline; also Ray-based, just not Ray Train).
    launcher: str = "ray_train"
    max_failures: int = 0
    # Env vars set in each worker process at launch (before torch/CUDA init).
    env_vars: Dict[str, str] = field(default_factory=dict)

    def grad_accum_steps(self, data_parallel_size: int) -> int:
        """Derive gradient-accumulation steps from the target global batch.

        Returns 1 when ``global_batch_size`` is unset. Raises if the global
        batch isn't divisible by micro_batch_size x data_parallel_size.
        """
        gbs = self.training.global_batch_size
        if not gbs:
            return 1
        denom = self.data.micro_batch_size * data_parallel_size
        if denom <= 0 or gbs % denom != 0:
            raise ValueError(
                f"global_batch_size={gbs} not divisible by micro_batch_size x "
                f"data_parallel_size ({self.data.micro_batch_size} x "
                f"{data_parallel_size} = {denom})."
            )
        return max(1, gbs // denom)

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def _build_dataclass(cls, data: Optional[Dict[str, Any]]):
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


# Nested config sections and their dataclasses.
_SECTIONS = {
    "model": ModelConfig,
    "data": DataConfig,
    "training": TrainingConfig,
    "scaling": ScalingConfig,
    "checkpoint": CheckpointConfig,
    "validation": ValidationConfig,
}


def experiment_from_dict(raw: Dict[str, Any]) -> ExperimentConfig:
    raw = dict(raw)
    sections = {
        name: _build_dataclass(cls, raw.pop(name, None))
        for name, cls in _SECTIONS.items()
    }

    perturbation_raw = raw.pop("perturbation", None)
    perturbation = (
        _build_dataclass(PerturbationConfig, perturbation_raw)
        if perturbation_raw
        else None
    )

    cfg = ExperimentConfig(perturbation=perturbation, **sections, **raw)

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
