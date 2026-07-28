import logging
import time
from typing import List, Optional

import ray
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxTimeoutError
from ray.experimental.sandbox.sandbox import Sandbox

logger = logging.getLogger(__name__)


@ray.remote
class SandboxPoolActor:
    """Ray Actor managing a pool of pre-warmed Sandboxes for high-throughput RL."""

    def __init__(self, size: int, config: SandboxConfig):
        self.size = size
        self.config = config
        self.backend = GVisorSandboxBackend()
        self.available_ids: List[str] = []
        self.busy_ids: List[str] = []
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Pre-warm the sandbox pool up to target size."""
        logger.info(f"Initializing SandboxPool of size {self.size}...")
        for _ in range(self.size):
            try:
                sb_id = self.backend.create_sandbox(self.config)
                self.available_ids.append(sb_id)
            except Exception as err:
                logger.error(f"Failed to pre-warm sandbox: {err}")

    def acquire(self, timeout: float = 30.0) -> str:
        """Acquire a ready warm sandbox ID from the pool."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.available_ids:
                sb_id = self.available_ids.pop(0)
                self.busy_ids.append(sb_id)
                return sb_id
            time.sleep(0.1)
        raise SandboxTimeoutError(
            f"No available sandboxes in pool after {timeout} seconds."
        )

    def release(self, sandbox_id: str, recycle: bool = True) -> None:
        """Return a sandbox to the pool, optionally recycling its workspace."""
        if sandbox_id in self.busy_ids:
            self.busy_ids.remove(sandbox_id)

        if recycle:
            try:
                # Reset workspace directory
                self.backend.exec_command(
                    sandbox_id,
                    f"rm -rf {self.config.work_dir}/* && mkdir -p {self.config.work_dir}",
                )
                self.available_ids.append(sandbox_id)
                return
            except Exception as err:
                logger.warning(
                    f"Failed to recycle sandbox '{sandbox_id}', deleting and replacing: {err}"
                )

        # Delete and replace if recycling failed or disabled
        self.backend.delete_sandbox(sandbox_id)
        try:
            new_id = self.backend.create_sandbox(self.config)
            self.available_ids.append(new_id)
        except Exception as err:
            logger.error(f"Failed to create replacement sandbox: {err}")

    def get_stats(self) -> dict:
        """Return pool capacity and utilization metrics."""
        return {
            "target_size": self.size,
            "available_count": len(self.available_ids),
            "busy_count": len(self.busy_ids),
        }

    def shutdown(self) -> None:
        """Terminate all pooled sandboxes."""
        all_ids = self.available_ids + self.busy_ids
        for sb_id in all_ids:
            try:
                self.backend.delete_sandbox(sb_id)
            except Exception:
                pass
        self.available_ids.clear()
        self.busy_ids.clear()


class SandboxPool:
    """Client interface for interacting with a Ray SandboxPoolActor."""

    def __init__(self, size: int, config: Optional[SandboxConfig] = None):
        self.config = config or SandboxConfig()
        self.actor = SandboxPoolActor.remote(size=size, config=self.config)

    def acquire(self, timeout: float = 30.0) -> Sandbox:
        """Acquire a ready warm sandbox instance from the pool."""
        sb_id = ray.get(self.actor.acquire.remote(timeout=timeout))
        backend = GVisorSandboxBackend()
        return Sandbox(sandbox_id=sb_id, backend=backend, config=self.config)

    def release(self, sandbox: Sandbox, recycle: bool = True) -> None:
        """Return a sandbox to the pool."""
        ray.get(self.actor.release.remote(sandbox.sandbox_id, recycle=recycle))

    def get_stats(self) -> dict:
        """Get pool stats."""
        return ray.get(self.actor.get_stats.remote())

    def close(self) -> None:
        """Shut down the pool and release all resources."""
        ray.get(self.actor.shutdown.remote())
        ray.kill(self.actor)
