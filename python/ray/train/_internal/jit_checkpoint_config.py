"""Configuration for Just-In-Time (JIT) Checkpointing in Ray Train."""

from dataclasses import dataclass

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class JITCheckpointConfig:
    """Configuration for Just-In-Time checkpointing.

    JIT checkpointing saves a checkpoint asynchronously when a SIGTERM signal
    is received, allowing recovery from preemption with minimal data loss.

    This is particularly useful for:
    - Preemptible workloads (Kubernetes preemption, spot instances)
    - Jobs with infrequent scheduled checkpoints
    - Reducing wasted work when workers are gracefully terminated

    Limitations:
    - Only handles SIGTERM (graceful termination signals)
    - Does NOT handle SIGKILL (immediate kill, no time to checkpoint)
    - Does NOT handle node failures (use regular checkpoints for that)
    - Best suited for cloud environments that send SIGTERM before termination

    Example:
        Enable via environment variables (recommended):
            $ export RAY_TRAIN_JIT_CHECKPOINT_ENABLED=1
            $ export RAY_TRAIN_JIT_CHECKPOINT_KILL_WAIT=5.0
            $ python train_script.py

        Or programmatically via RunConfig (deprecated):
            >>> from ray.train import RunConfig
            >>> from ray.train._internal.jit_checkpoint_config import JITCheckpointConfig
            >>> config = RunConfig(
            ...     jit_checkpoint_config=JITCheckpointConfig(
            ...         enabled=True,
            ...         kill_wait=3.0
            ...     )
            ... )

    Args:
        enabled: Whether to enable JIT checkpointing. Defaults to False.
            When enabled, workers will listen for SIGTERM signals and
            automatically trigger a checkpoint save before termination.
        kill_wait: Seconds to wait after SIGTERM before starting checkpoint.
            This optimization avoids wasting time if SIGKILL follows
            immediately after SIGTERM (common in some cluster managers).
            Defaults to 3.0 seconds.
    """

    enabled: bool = False
    kill_wait: float = 3.0

    def __post_init__(self):
        """Validate configuration parameters."""
        if self.kill_wait < 0:
            raise ValueError(f"kill_wait must be non-negative, got {self.kill_wait}")
        if self.kill_wait > 60:
            # Warn about very long kill_wait times
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"kill_wait={self.kill_wait}s is unusually long. "
                "Most cluster managers send SIGKILL within 30s of SIGTERM."
            )
