"""DeepSpeed framework adapter for LLM benchmarks.

Builds a DeepSpeed config from an ``ExperimentConfig`` (ZeRO stage from
``model.parallelism``, precision, optimizer, gradient accumulation), loads an
HF causal-LM, and runs a step-bounded training loop while collecting
tokens/sec, model TFLOPs, and MFU via ``TrainMetricsCollector``.

Derived from the Ray Train DeepSpeed fine-tuning example
(doc/source/train/examples/pytorch/deepspeed_finetune/train.py), generalized
so the workload is fully driven by the experiment YAML.
"""

import logging
import os
import tempfile
from typing import Any, Dict, Optional

import torch

from core.experiment_config import ExperimentConfig
from core.metrics import (
    FlopsSpec,
    GpuTrainMetricsCollector,
    TrainMetricsCollector,
    flops_per_token,
    get_gpu_peak_bandwidth_gbps,
    get_gpu_peak_flops,
)
from core.train_context import TrainContext
from data.text_dataset import build_text_dataloader
from frameworks.base_adapter import FrameworkAdapter

logger = logging.getLogger(__name__)


# Config attribute aliases across HF MoE model families.
_NUM_EXPERTS_ATTRS = ("num_experts", "num_local_experts", "n_routed_experts")
_TOP_K_ATTRS = ("num_experts_per_tok", "moe_topk", "num_experts_per_token", "topk")
# model_type substrings that indicate non-quadratic (linear/recurrent) attention.
_LINEAR_ATTENTION_HINTS = ("deltanet", "mamba", "rwkv", "retnet", "linear", "lightning")


def _first_attr(config, names):
    for name in names:
        value = getattr(config, name, None)
        if value:
            return value
    return None


def _count_active_params(model, config) -> int:
    """Params that fire per token: total for dense; for MoE, non-expert params +
    top_k/num_experts of the routed-expert params (shared experts stay active).

    Counts expert tensors from the model by name (robust across SwiGLU/GQA/expert
    sizing) rather than re-deriving shapes from config.
    """
    total = sum(p.numel() for p in model.parameters())
    num_experts = _first_attr(config, _NUM_EXPERTS_ATTRS)
    top_k = _first_attr(config, _TOP_K_ATTRS)
    if not num_experts or not top_k:
        return total  # dense

    # Routed experts are named like "...experts.<i>...". Shared experts use a
    # different name (e.g. "shared_expert") so they're excluded here and stay
    # fully counted in non-expert params — correct, since they're always active.
    routed_expert_params = sum(
        p.numel() for n, p in model.named_parameters() if ".experts." in n
    )
    if routed_expert_params == 0:
        return total
    active_expert_params = routed_expert_params * top_k / num_experts
    return int(total - routed_expert_params + active_expert_params)


def _detect_attention_kind(config) -> str:
    """Pick the FLOPs attention term automatically from the HF config.

    Defaults to "quadratic"; returns "linear" for DeltaNet/SSM/RWKV-style
    families (where the quadratic seq term would be wrong).
    """
    model_type = (getattr(config, "model_type", "") or "").lower()
    if any(hint in model_type for hint in _LINEAR_ATTENTION_HINTS):
        return "linear"
    if getattr(config, "linear_attention", False):
        return "linear"
    # Hybrid models may list per-layer types; if any are linear, treat as linear
    # (approximate — the quadratic term would over-count these).
    layer_types = getattr(config, "layer_types", None)
    if layer_types and any(
        ("linear" in str(t).lower() or "delta" in str(t).lower()) for t in layer_types
    ):
        return "linear"
    return "quadratic"


class DeepSpeedAdapter(FrameworkAdapter):
    def __init__(self, cfg: ExperimentConfig, ctx: TrainContext):
        super().__init__(cfg, ctx)
        self._tokenizer = None
        self._hf_config = None
        self._num_params: Optional[int] = None
        self._active_params: Optional[int] = None
        self._attention_kind: str = "quadratic"

    # ---- config construction -------------------------------------------------

    def _precision_config(self) -> Dict[str, Any]:
        """Map the experiment precision to DeepSpeed fp16/bf16 blocks.

        Falls back from bf16 to fp16 on GPUs without bf16 support (e.g. T4),
        matching the doc example, so the same YAML runs across GPU classes.
        """
        precision = self.cfg.model.precision
        if precision == "bf16":
            if torch.cuda.is_available() and not torch.cuda.is_bf16_supported():
                logger.warning("bf16 unsupported on this GPU; falling back to fp16.")
                return {"fp16": {"enabled": True}}
            return {"bf16": {"enabled": True}}
        if precision == "fp16":
            return {"fp16": {"enabled": True}}
        return {}  # fp32

    def _build_ds_config(self) -> Dict[str, Any]:
        parallelism = self.cfg.model.parallelism or {}
        zero_stage = parallelism.get("zero_stage", 3)

        ds_config: Dict[str, Any] = {
            "train_micro_batch_size_per_gpu": self.cfg.data.micro_batch_size,
            # DeepSpeed ZeRO is pure data-parallel, so data_parallel_size =
            # world_size; grad-accum is derived from the target global batch.
            "gradient_accumulation_steps": self.cfg.grad_accum_steps(
                data_parallel_size=self.ctx.world_size
            ),
            "zero_optimization": {
                "stage": zero_stage,
                "overlap_comm": True,
                "contiguous_gradients": True,
            },
            "gradient_clipping": 1.0,
        }
        ds_config.update(self._precision_config())
        # Framework-native overrides win, so a ds_config.json can fully
        # customize without code changes.
        ds_config.update(self.cfg.model.framework_config or {})
        return ds_config

    # ---- model / data --------------------------------------------------------

    def _build_engine(self):
        import deepspeed
        from transformers import AutoConfig, AutoModelForCausalLM

        model_name = self.cfg.model.name
        self._hf_config = AutoConfig.from_pretrained(model_name)
        model = AutoModelForCausalLM.from_pretrained(
            model_name, attn_implementation=self.cfg.model.attn_implementation
        )

        if self.cfg.model.gradient_checkpointing:
            # use_cache must be off for gradient checkpointing (the KV cache and
            # recompute are mutually exclusive); HF warns and disables it anyway.
            model.config.use_cache = False
            model.gradient_checkpointing_enable()

        self._num_params = sum(p.numel() for p in model.parameters())
        self._active_params = _count_active_params(model, self._hf_config)
        self._attention_kind = _detect_attention_kind(self._hf_config)
        if self.ctx.world_rank == 0:
            moe = self._active_params != self._num_params
            logger.info(
                f"Loaded {model_name}: {self._num_params/1e9:.3f}B params"
                + (f" ({self._active_params/1e9:.3f}B active, MoE)" if moe else "")
                + f" | attn_flops={self._attention_kind}, "
                f"attn_impl={self.cfg.model.attn_implementation}, "
                f"grad_ckpt={self.cfg.model.gradient_checkpointing}"
            )
            if self._attention_kind == "linear":
                logger.warning(
                    "Linear-attention model: attention FLOPs omitted from MFU "
                    "(conservative underestimate)."
                )

        opt = self.cfg.training.optimizer
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=opt.get("lr", 3e-4),
            betas=tuple(opt.get("betas", (0.9, 0.95))),
            weight_decay=opt.get("weight_decay", 0.1),
        )
        engine, _, _, _ = deepspeed.initialize(
            model=model, optimizer=optimizer, config=self._build_ds_config()
        )
        return engine

    def _build_tokenizer(self):
        if self.cfg.data.dataset == "synthetic":
            return None
        from transformers import AutoTokenizer

        tokenizer = AutoTokenizer.from_pretrained(
            self.cfg.model.name, trust_remote_code=True
        )
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token or tokenizer.unk_token
        return tokenizer

    # ---- FrameworkAdapter API ------------------------------------------------

    def flops_per_token(self) -> Optional[float]:
        if self._active_params is None or self._hf_config is None:
            return None
        return flops_per_token(
            FlopsSpec(
                active_params=self._active_params,
                num_layers=self._hf_config.num_hidden_layers,
                hidden_size=self._hf_config.hidden_size,
                seq_len=self.cfg.data.seq_len,
                attention=self._attention_kind,
            )
        )

    def _maybe_checkpoint(self, engine, step: int) -> None:
        interval = self.cfg.checkpoint.every_n_steps
        if interval <= 0 or step % interval != 0:
            return
        with tempfile.TemporaryDirectory() as tmp_dir:
            ckpt_dir = os.path.join(tmp_dir, "checkpoint")
            os.makedirs(ckpt_dir, exist_ok=True)
            engine.save_checkpoint(ckpt_dir)
            self.ctx.report({"step": step}, checkpoint_dir=tmp_dir)

    def _resolve_num_steps(self, dataloader) -> int:
        """num_steps directly, else derive from num_epochs x steps-per-epoch.

        Epoch mode needs a finite (map-style) dataset; the synthetic/streaming
        loaders are infinite, so num_steps must be set for those.
        """
        if self.cfg.training.num_steps is not None:
            return self.cfg.training.num_steps
        try:
            steps_per_epoch = len(dataloader)
        except TypeError:
            raise ValueError(
                "num_epochs needs a finite dataset; set training.num_steps for "
                "synthetic/streaming data."
            )
        return self.cfg.training.num_epochs * steps_per_epoch

    def run(self) -> None:
        torch.manual_seed(self.cfg.training.seed)

        self._tokenizer = self._build_tokenizer()
        engine = self._build_engine()
        device = self.ctx.device()

        peak_flops = None
        if torch.cuda.is_available():
            peak_flops = get_gpu_peak_flops(
                torch.cuda.get_device_name(device), self.cfg.model.precision
            )

        if torch.cuda.is_available():
            # Logical CUDA index — 0 when CUDA_VISIBLE_DEVICES restricts the
            # process to one GPU (Ray/torchrun); the NVML sampler maps it back to
            # the physical index via CVD.
            collector = GpuTrainMetricsCollector(
                world_size=self.ctx.world_size,
                warmup_steps=self.cfg.training.warmup_steps,
                flops_per_token=self.flops_per_token(),
                peak_flops_per_gpu=peak_flops,
                device=device,
                gpu_index=getattr(device, "index", None) or 0,
            )
        else:
            collector = TrainMetricsCollector(
                world_size=self.ctx.world_size,
                warmup_steps=self.cfg.training.warmup_steps,
                flops_per_token=self.flops_per_token(),
                peak_flops_per_gpu=peak_flops,
            )

        batch_size = self.cfg.data.micro_batch_size
        dataloader = build_text_dataloader(
            dataset_name=self.cfg.data.dataset,
            dataset_path=self.cfg.data.dataset_path,
            tokenizer=self._tokenizer,
            seq_len=self.cfg.data.seq_len,
            batch_size=batch_size,
            seed=self.cfg.training.seed,
            limit_rows=self.cfg.data.limit_training_rows,
        )

        num_steps = self._resolve_num_steps(dataloader)
        seq_len = self.cfg.data.seq_len
        engine.train()

        data_iter = iter(dataloader)
        step = 0
        while step < num_steps:
            with collector.data_timer.timer():
                try:
                    batch = next(data_iter)
                except StopIteration:
                    data_iter = iter(dataloader)
                    batch = next(data_iter)

            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)

            with collector.step_timer.timer():
                outputs = engine(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    labels=input_ids,
                    use_cache=False,
                )
                loss = outputs.loss
                engine.backward(loss)
                engine.step()

            collector.record_batch(num_rows=batch_size, num_tokens=batch_size * seq_len)
            step += 1

            if (
                step % self.cfg.training.log_every_n_steps == 0
                and self.ctx.world_rank == 0
            ):
                logger.info(f"step {step}/{num_steps} loss={loss.item():.4f}")

            self._maybe_checkpoint(engine, step)

        metrics = collector.summary()
        metrics["loss"] = loss.item()
        metrics["num_params"] = self._num_params
        metrics["active_params"] = self._active_params
        # dense vs MoE is an explicit benchmark dimension.
        is_moe = self._active_params != self._num_params
        metrics["config/model_kind"] = "moe" if is_moe else "dense"
        metrics["config/attention_flops"] = self._attention_kind

        # Self-describing config echo so results JSON renders into a table
        # (collect.py) and archives without needing the source YAML.
        grad_accum = self.cfg.grad_accum_steps(data_parallel_size=self.ctx.world_size)
        metrics["config/model"] = self.cfg.model.name
        metrics["config/adapter"] = self.cfg.adapter
        metrics["config/precision"] = self.cfg.model.precision
        metrics["config/zero_stage"] = (self.cfg.model.parallelism or {}).get(
            "zero_stage"
        )
        metrics["config/gradient_checkpointing"] = self.cfg.model.gradient_checkpointing
        metrics["config/seq_len"] = seq_len
        metrics["config/micro_batch_size"] = batch_size
        metrics["config/grad_accum_steps"] = grad_accum
        metrics["config/global_batch_size"] = (
            batch_size * self.ctx.world_size * grad_accum
        )
        if torch.cuda.is_available():
            gpu_name = torch.cuda.get_device_name(device)
            metrics["config/gpu"] = gpu_name
            peak_bw = get_gpu_peak_bandwidth_gbps(gpu_name)
            if peak_bw is not None:
                metrics["gpu/peak_memory_bandwidth_gbps"] = peak_bw

        self.ctx.report(metrics)
        if self.ctx.world_rank == 0:
            logger.info(f"Final metrics: {metrics}")
        return metrics
