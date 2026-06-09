"""Benchmark metrics: throughput, FLOPs/MFU estimation, and GPU utilization.

Fills the gaps called out in the benchmark modernization proposal: the legacy
harness only collected per-step timers and rows/sec; this module adds
tokens/sec, model FLOPs, MFU, and sampled GPU utilization/memory.
"""

import logging
import statistics
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# Peak dense (no-sparsity) tensor-core FLOP/s by GPU product name substring.
# Sources: NVIDIA datasheets. Matched case-insensitively against
# torch.cuda.get_device_name(); first hit wins, so keep more specific names
# (e.g. "h100 nvl") before generic ones.
GPU_PEAK_FLOPS: Dict[str, Dict[str, float]] = {
    "b200": {"bf16": 2250e12, "fp16": 2250e12, "fp32": 80e12},
    "h200": {"bf16": 989e12, "fp16": 989e12, "fp32": 67e12},
    "h100 nvl": {"bf16": 835e12, "fp16": 835e12, "fp32": 60e12},
    "h100 pcie": {"bf16": 756e12, "fp16": 756e12, "fp32": 51e12},
    "h100": {"bf16": 989e12, "fp16": 989e12, "fp32": 67e12},  # SXM
    "a100": {"bf16": 312e12, "fp16": 312e12, "fp32": 19.5e12},
    "a10g": {"bf16": 70e12, "fp16": 70e12, "fp32": 31.2e12},
    "l40s": {"bf16": 362e12, "fp16": 362e12, "fp32": 91.6e12},
    "l4": {"bf16": 121e12, "fp16": 121e12, "fp32": 30.3e12},
    "v100": {"fp16": 125e12, "fp32": 15.7e12},
    "t4": {"fp16": 65e12, "fp32": 8.1e12},
}


def get_gpu_peak_flops(device_name: str, precision: str) -> Optional[float]:
    """Look up peak dense FLOP/s for a device name + precision ("bf16" etc.).

    Returns None when the device or precision is unknown, in which case MFU
    is reported as unavailable rather than silently wrong.
    """
    name = device_name.lower()
    for key, by_precision in GPU_PEAK_FLOPS.items():
        if key in name:
            peak = by_precision.get(precision)
            if peak is None:
                logger.warning(
                    f"No {precision} peak FLOPs known for device '{device_name}'."
                )
            return peak
    logger.warning(f"Unknown device '{device_name}' for peak FLOPs lookup.")
    return None


def transformer_flops_per_token(
    num_params: int,
    num_layers: int,
    hidden_size: int,
    seq_len: int,
    include_backward: bool = True,
) -> float:
    """Approximate train FLOPs per token for a decoder-only transformer.

    Uses the standard 6N approximation (forward 2N + backward 4N) plus the
    attention score/value matmuls which the parameter-count term misses:
    12 * num_layers * hidden_size * seq_len  (= 6 * 2 * L * d * T).
    Matches the accounting used by PaLM (Appendix B) and llm-foundry's MFU
    tables, so results are comparable across benchmark reports.
    """
    factor = 6 if include_backward else 2
    attn_factor = 12 if include_backward else 4
    return factor * num_params + attn_factor * num_layers * hidden_size * seq_len


def model_flops_per_token_from_hf_config(
    num_params: int, hf_config: Any, seq_len: int
) -> float:
    """Convenience wrapper reading layer/hidden dims off a HF PretrainedConfig."""
    return transformer_flops_per_token(
        num_params=num_params,
        num_layers=getattr(hf_config, "num_hidden_layers"),
        hidden_size=getattr(hf_config, "hidden_size"),
        seq_len=seq_len,
    )


class GpuMonitor:
    """Samples GPU utilization and memory in a background thread via NVML.

    No-ops cleanly when pynvml / a GPU is unavailable so the harness can run
    on CPU for smoke tests.
    """

    def __init__(self, device_index: int = 0, interval_s: float = 1.0):
        self._device_index = device_index
        self._interval_s = interval_s
        self._utilization: List[float] = []
        self._memory_used_gb: List[float] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._nvml = None

    def start(self) -> None:
        try:
            import pynvml

            pynvml.nvmlInit()
            self._nvml = pynvml
        except Exception as e:
            logger.warning(f"GPU monitoring disabled (pynvml unavailable: {e})")
            return

        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _sample_loop(self) -> None:
        handle = self._nvml.nvmlDeviceGetHandleByIndex(self._device_index)
        while not self._stop_event.is_set():
            try:
                util = self._nvml.nvmlDeviceGetUtilizationRates(handle)
                mem = self._nvml.nvmlDeviceGetMemoryInfo(handle)
                self._utilization.append(float(util.gpu))
                self._memory_used_gb.append(mem.used / 1e9)
            except Exception:
                pass
            self._stop_event.wait(self._interval_s)

    def stop(self) -> Dict[str, float]:
        """Stop sampling and return summary stats (empty dict when disabled)."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self._nvml is not None:
            try:
                self._nvml.nvmlShutdown()
            except Exception:
                pass
        if not self._utilization:
            return {}
        return {
            "gpu/utilization_mean_pct": statistics.fmean(self._utilization),
            "gpu/utilization_max_pct": max(self._utilization),
            "gpu/memory_used_gb_max": max(self._memory_used_gb),
            "gpu/num_samples": len(self._utilization),
        }


class StepTimer:
    """Accumulates per-step wall times, split into warmup and steady state."""

    def __init__(self, warmup_steps: int):
        self._warmup_steps = warmup_steps
        self._times: List[float] = []

    @contextmanager
    def timer(self):
        start = time.perf_counter()
        yield
        self._times.append(time.perf_counter() - start)

    @property
    def num_steps(self) -> int:
        return len(self._times)

    @property
    def steady_state_times(self) -> List[float]:
        return self._times[self._warmup_steps :]

    def total(self, include_warmup: bool = True) -> float:
        return sum(self._times if include_warmup else self.steady_state_times)

    def summary(self, prefix: str) -> Dict[str, float]:
        if not self._times:
            return {}
        out = {
            f"{prefix}/step_time_total_s": sum(self._times),
            f"{prefix}/num_steps": len(self._times),
        }
        steady = self.steady_state_times
        if steady:
            out.update(
                {
                    f"{prefix}/step_time_mean_s": statistics.fmean(steady),
                    f"{prefix}/step_time_p50_s": statistics.median(steady),
                    f"{prefix}/step_time_max_s": max(steady),
                }
            )
        return out


class TrainMetricsCollector:
    """Aggregates throughput / FLOPs / MFU / GPU stats for one training run.

    Lives inside the training worker. Steady-state metrics (tokens/sec, MFU)
    exclude the first ``warmup_steps`` steps so model download, compilation,
    and allocator warmup don't skew steady-state throughput.
    """

    def __init__(
        self,
        world_size: int,
        warmup_steps: int = 5,
        flops_per_token: Optional[float] = None,
        peak_flops_per_gpu: Optional[float] = None,
        gpu_index: int = 0,
        monitor_gpu: bool = True,
    ):
        self.world_size = world_size
        self.flops_per_token = flops_per_token
        self.peak_flops_per_gpu = peak_flops_per_gpu

        self.step_timer = StepTimer(warmup_steps)
        self.data_timer = StepTimer(warmup_steps)
        self._warmup_steps = warmup_steps
        self._tokens_per_step: List[float] = []
        self._rows_per_step: List[float] = []
        self._run_start = time.perf_counter()

        self._gpu_monitor = GpuMonitor(device_index=gpu_index) if monitor_gpu else None
        if self._gpu_monitor:
            self._gpu_monitor.start()

    def record_batch(self, num_rows: int, num_tokens: Optional[int] = None) -> None:
        """Record the per-device batch that the just-timed step consumed."""
        self._rows_per_step.append(num_rows)
        if num_tokens is not None:
            self._tokens_per_step.append(num_tokens)

    def summary(self) -> Dict[str, float]:
        metrics: Dict[str, float] = {
            "e2e_time_s": time.perf_counter() - self._run_start,
            "world_size": self.world_size,
        }
        metrics.update(self.step_timer.summary("train"))
        metrics.update(self.data_timer.summary("data"))

        steady_step_times = self.step_timer.steady_state_times
        steady_time = sum(steady_step_times)
        if steady_time > 0:
            n_steady = len(steady_step_times)
            steady_rows = sum(self._rows_per_step[-n_steady:])
            # Local (per-device) rates; global assumes symmetric data-parallel
            # workers, which holds for every adapter in this harness today.
            metrics["train/rows_per_sec_per_device"] = steady_rows / steady_time
            metrics["train/global_rows_per_sec"] = (
                metrics["train/rows_per_sec_per_device"] * self.world_size
            )

            if self._tokens_per_step:
                steady_tokens = sum(self._tokens_per_step[-n_steady:])
                tokens_per_sec_device = steady_tokens / steady_time
                metrics["train/tokens_per_sec_per_device"] = tokens_per_sec_device
                metrics["train/global_tokens_per_sec"] = (
                    tokens_per_sec_device * self.world_size
                )

                if self.flops_per_token:
                    achieved = tokens_per_sec_device * self.flops_per_token
                    metrics["train/model_tflops_per_sec_per_device"] = achieved / 1e12
                    if self.peak_flops_per_gpu:
                        metrics["train/mfu"] = achieved / self.peak_flops_per_gpu

        if self._gpu_monitor:
            metrics.update(self._gpu_monitor.stop())
            self._gpu_monitor = None
        return metrics
