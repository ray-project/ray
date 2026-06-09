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
    TrainMetricsCollector,
    get_gpu_peak_flops,
    model_flops_per_token_from_hf_config,
)
from core.train_context import TrainContext
from data.text_dataset import build_text_dataloader
from frameworks.base_adapter import FrameworkAdapter

logger = logging.getLogger(__name__)


class DeepSpeedAdapter(FrameworkAdapter):
    def __init__(self, cfg: ExperimentConfig, ctx: TrainContext):
        super().__init__(cfg, ctx)
        self._tokenizer = None
        self._hf_config = None
        self._num_params: Optional[int] = None

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
        training = self.cfg.training

        ds_config: Dict[str, Any] = {
            "train_micro_batch_size_per_gpu": self.cfg.data.batch_size,
            "gradient_accumulation_steps": training.gradient_accumulation_steps,
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
        model = AutoModelForCausalLM.from_pretrained(model_name)
        self._num_params = sum(p.numel() for p in model.parameters())
        if self.ctx.world_rank == 0:
            logger.info(f"Loaded {model_name}: {self._num_params/1e9:.3f}B params")

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
        if self._num_params is None or self._hf_config is None:
            return None
        return model_flops_per_token_from_hf_config(
            self._num_params, self._hf_config, self.cfg.data.seq_len
        )

    def _maybe_checkpoint(self, engine, step: int) -> None:
        interval = self.cfg.checkpoint.interval
        if interval <= 0 or step % interval != 0:
            return
        with tempfile.TemporaryDirectory() as tmp_dir:
            ckpt_dir = os.path.join(tmp_dir, "checkpoint")
            os.makedirs(ckpt_dir, exist_ok=True)
            engine.save_checkpoint(ckpt_dir)
            self.ctx.report({"step": step}, checkpoint_dir=tmp_dir)

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

        collector = TrainMetricsCollector(
            world_size=self.ctx.world_size,
            warmup_steps=self.cfg.training.warmup_steps,
            flops_per_token=self.flops_per_token(),
            peak_flops_per_gpu=peak_flops,
            gpu_index=self.ctx.local_rank,
            monitor_gpu=torch.cuda.is_available(),
        )

        dataloader = build_text_dataloader(
            dataset_name=self.cfg.data.dataset,
            dataset_path=self.cfg.data.dataset_path,
            tokenizer=self._tokenizer,
            seq_len=self.cfg.data.seq_len,
            batch_size=self.cfg.data.batch_size,
            seed=self.cfg.training.seed,
            limit_rows=self.cfg.data.limit_training_rows,
        )

        num_steps = self.cfg.training.num_steps
        seq_len = self.cfg.data.seq_len
        batch_size = self.cfg.data.batch_size
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

            collector.record_batch(
                num_rows=batch_size, num_tokens=batch_size * seq_len
            )
            step += 1

            if step % self.cfg.training.log_every_n_steps == 0 and self.ctx.world_rank == 0:
                logger.info(f"step {step}/{num_steps} loss={loss.item():.4f}")

            self._maybe_checkpoint(engine, step)

        metrics = collector.summary()
        metrics["loss"] = loss.item()
        metrics["num_params"] = self._num_params
        self.ctx.report(metrics)
        if self.ctx.world_rank == 0:
            logger.info(f"Final metrics: {metrics}")
        return metrics
