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


# MoE routing attrs, scoped to the Qwen configs this harness benchmarks.
# Other families spell these differently (Mixtral: num_local_experts,
# DeepSeek: n_routed_experts); extend deliberately when such a model lands —
# _count_active_params warns rather than silently over-counting FLOPs.
_NUM_EXPERTS_ATTRS = ("num_experts",)
_TOP_K_ATTRS = ("num_experts_per_tok",)


def _deep_update(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge ``overrides`` into ``base`` (nested dicts merge,
    scalars/lists replace). A shallow update would let a partial override like
    ``zero_optimization.offload_optimizer`` silently drop the adapter-built
    ``stage``/``overlap_comm`` keys.
    """
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _first_attr(config, names):
    # `is not None` (not truthiness) so a legitimate 0 isn't skipped — a falsy
    # check would silently fall through to the next alias / None.
    for name in names:
        value = getattr(config, name, None)
        if value is not None:
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
        # Guardrail for the Qwen-scoped alias lists above: a config that looks
        # MoE but isn't recognized falls back to dense, which overstates
        # MFU/HFU — say so instead of failing silently.
        known = set(_NUM_EXPERTS_ATTRS + _TOP_K_ATTRS)
        unrecognized = [
            k for k in vars(config) if "expert" in k.lower() and k not in known
        ]
        if unrecognized or num_experts or top_k:
            logger.warning(
                f"Config has MoE-like attrs (unrecognized={unrecognized}, "
                f"num_experts={num_experts}, top_k={top_k}) but no complete "
                "routing spec in the Qwen-scoped alias lists; counting the "
                "model as dense — MFU/HFU may be overstated. Extend "
                "_NUM_EXPERTS_ATTRS/_TOP_K_ATTRS for this model family."
            )
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

    Defaults to "quadratic"; returns "linear" when the config declares it —
    Qwen3.5's hybrid Gated-DeltaNet layers show up via ``layer_types`` (where
    the quadratic seq term would be wrong).
    """
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

    def _effective_precision(self) -> str:
        """The precision that actually runs: bf16 falls back to fp16 on GPUs
        without bf16 support (e.g. T4), so the same YAML runs across GPU
        classes. Everything precision-dependent (the DeepSpeed config AND the
        peak-FLOPs lookup for MFU/HFU) must use this, not the configured value.
        """
        precision = self.cfg.model.precision
        if (
            precision == "bf16"
            and torch.cuda.is_available()
            and not torch.cuda.is_bf16_supported()
        ):
            return "fp16"
        return precision

    def _precision_config(self) -> Dict[str, Any]:
        """Map the effective precision to DeepSpeed fp16/bf16 blocks."""
        precision = self._effective_precision()
        if precision != self.cfg.model.precision:
            logger.warning(
                f"{self.cfg.model.precision} unsupported on this GPU; "
                f"falling back to {precision}."
            )
        if precision == "bf16":
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
        # Framework-native overrides win, deep-merged so a partial override
        # (e.g. just zero_optimization.offload_optimizer) refines the
        # adapter-built config instead of clobbering whole nested blocks.
        _deep_update(ds_config, self.cfg.model.framework_config or {})
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

    def hardware_flops_per_token(self) -> Optional[float]:
        """Model FLOPs plus recompute: the HFU numerator.

        HF gradient checkpointing recomputes every decoder layer's forward
        during backward, so the hardware executes one extra forward per token
        (6N -> 8N, PaLM Appendix B accounting). Without checkpointing,
        HFU == MFU. Uses active params, so MoE recompute is counted correctly.
        """
        model_flops = self.flops_per_token()
        if model_flops is None or not self.cfg.model.gradient_checkpointing:
            return model_flops
        extra_forward = flops_per_token(
            FlopsSpec(
                active_params=self._active_params,
                num_layers=self._hf_config.num_hidden_layers,
                hidden_size=self._hf_config.hidden_size,
                seq_len=self.cfg.data.seq_len,
                attention=self._attention_kind,
                include_backward=False,
            )
        )
        return model_flops + extra_forward

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

    def _train_phase(self, device) -> "tuple[TrainMetricsCollector, Optional[float]]":
        """Build the engine + dataloader and run the step loop.

        Split from ``run`` so an OOM anywhere in it (model load, ZeRO init, or
        a training step) can be caught and reported as a benchmark result.
        """
        self._tokenizer = self._build_tokenizer()
        engine = self._build_engine()

        peak_flops = None
        if torch.cuda.is_available():
            # Effective (not configured) precision: on a bf16->fp16 fallback
            # the fp16 peak is what the silicon can actually do.
            peak_flops = get_gpu_peak_flops(
                torch.cuda.get_device_name(device), self._effective_precision()
            )

        if torch.cuda.is_available():
            # Logical CUDA index — 0 when CUDA_VISIBLE_DEVICES restricts the
            # process to one GPU (both launchers); the NVML sampler maps it back to
            # the physical index via CVD.
            collector = GpuTrainMetricsCollector(
                world_size=self.ctx.world_size,
                warmup_steps=self.cfg.training.warmup_steps,
                flops_per_token=self.flops_per_token(),
                hardware_flops_per_token=self.hardware_flops_per_token(),
                peak_flops_per_gpu=peak_flops,
                device=device,
                gpu_index=getattr(device, "index", None) or 0,
            )
        else:
            collector = TrainMetricsCollector(
                world_size=self.ctx.world_size,
                warmup_steps=self.cfg.training.warmup_steps,
                flops_per_token=self.flops_per_token(),
                hardware_flops_per_token=self.hardware_flops_per_token(),
                peak_flops_per_gpu=peak_flops,
            )

        batch_size = self.cfg.data.micro_batch_size
        dataloader = build_text_dataloader(
            dataset_name=self.cfg.data.dataset,
            tokenizer=self._tokenizer,
            seq_len=self.cfg.data.seq_len,
            batch_size=batch_size,
            seed=self.cfg.training.seed,
            limit_rows=self.cfg.data.limit_training_rows,
            rank=self.ctx.world_rank,
            world_size=self.ctx.world_size,
        )

        num_steps = self._resolve_num_steps(dataloader)
        if num_steps < 1:
            raise ValueError(f"num_steps must be >= 1, got {num_steps}.")
        engine.train()

        data_iter = iter(dataloader)
        step = 0
        epoch = 0
        last_loss = None
        while step < num_steps:
            with collector.data_timer.timer():
                try:
                    batch = next(data_iter)
                except StopIteration:
                    # New pass over the data: advance the DistributedSampler
                    # epoch so it reshuffles — otherwise every pass repeats
                    # the epoch-0 order.
                    epoch += 1
                    sampler = getattr(dataloader, "sampler", None)
                    if hasattr(sampler, "set_epoch"):
                        sampler.set_epoch(epoch)
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
                last_loss = loss.item()
            collector.record_batch(
                num_rows=input_ids.size(0), num_tokens=input_ids.numel()
            )
            step += 1

            if (
                step % self.cfg.training.log_every_n_steps == 0
                and self.ctx.world_rank == 0
            ):
                logger.info(f"step {step}/{num_steps} loss={last_loss:.4f}")

            self._maybe_checkpoint(engine, step)

        return collector, last_loss

    def run(self) -> Dict[str, Any]:
        torch.manual_seed(self.cfg.training.seed)

        if self.cfg.data.seq_len is None:
            raise ValueError("data.seq_len is required for the deepspeed adapter.")

        device = self.ctx.device()
        oom = False
        collector = None
        last_loss = None
        try:
            collector, last_loss = self._train_phase(device)
        except (torch.cuda.OutOfMemoryError, RuntimeError) as e:
            # Report OOM as a benchmark result (oom=true row) instead of
            # crashing, so batch/seq sweeps record which cells don't fit.
            # Best-effort in multi-worker runs: peer ranks blocked in
            # collectives still fail via NCCL timeout.
            is_oom = isinstance(e, torch.cuda.OutOfMemoryError) or (
                "out of memory" in str(e).lower()
            )
            if not is_oom:
                raise
            logger.exception("CUDA OOM; reporting an oom=true benchmark row")
            oom = True

        if collector is None:  # OOM before the collector existed
            collector = TrainMetricsCollector(
                world_size=self.ctx.world_size,
                warmup_steps=self.cfg.training.warmup_steps,
            )
        metrics = collector.summary()
        metrics["oom"] = oom
        if last_loss is not None:
            metrics["loss"] = last_loss
        metrics["num_params"] = self._num_params
        metrics["active_params"] = self._active_params
        # dense vs MoE is an explicit benchmark dimension.
        if self._active_params is not None:
            is_moe = self._active_params != self._num_params
            metrics["config/model_kind"] = "moe" if is_moe else "dense"
        metrics["config/attention_flops"] = self._attention_kind

        # Self-describing config echo so results JSON renders into a table
        # (collect.py) and archives without needing the source YAML.
        seq_len = self.cfg.data.seq_len
        batch_size = self.cfg.data.micro_batch_size
        grad_accum = self.cfg.grad_accum_steps(data_parallel_size=self.ctx.world_size)
        global_batch = batch_size * self.ctx.world_size * grad_accum
        metrics["config/model"] = self.cfg.model.name
        metrics["config/framework"] = self.cfg.framework
        metrics["config/launcher"] = self.cfg.launcher
        metrics["config/dataloader"] = self.cfg.data.dataloader
        metrics["config/precision"] = self.cfg.model.precision
        # What actually ran (bf16 may fall back to fp16 on pre-Ampere GPUs);
        # MFU/HFU peaks are computed against this.
        metrics["config/effective_precision"] = self._effective_precision()
        metrics["config/zero_stage"] = (self.cfg.model.parallelism or {}).get(
            "zero_stage"
        )
        metrics["config/gradient_checkpointing"] = self.cfg.model.gradient_checkpointing
        metrics["config/seq_len"] = seq_len
        metrics["config/micro_batch_size"] = batch_size
        metrics["config/grad_accum_steps"] = grad_accum
        metrics["config/global_batch_size"] = global_batch
        metrics["config/global_batch_tokens"] = global_batch * seq_len

        # Parallelism degrees in Megatron terms. DeepSpeed ZeRO is pure data
        # parallel (tp = pp = cp = 1): stage 3 shards parameters, so the DP
        # group counts as dp_shard; stages 0-2 replicate them (dp_replicate).
        # Invariant: world = dp_replicate * dp_shard * tp * pp * cp.
        zero_stage = (self.cfg.model.parallelism or {}).get("zero_stage", 3)
        shards_params = zero_stage is not None and int(zero_stage) >= 3
        metrics["config/dp_shard"] = self.ctx.world_size if shards_params else 1
        metrics["config/dp_replicate"] = 1 if shards_params else self.ctx.world_size
        metrics["config/tp"] = 1
        metrics["config/pp"] = 1
        metrics["config/cp"] = 1

        # ZeRO-Offload flags from the final (framework_config-merged) DS config.
        zero_opt = self._build_ds_config().get("zero_optimization", {})
        metrics["config/offload_optimizer"] = bool(zero_opt.get("offload_optimizer"))
        metrics["config/offload_param"] = bool(zero_opt.get("offload_param"))
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
