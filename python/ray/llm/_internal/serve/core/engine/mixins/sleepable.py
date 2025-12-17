"""Sleepable engine mixin protocol.

Defines the interface for engines that support sleep/wakeup functionality.
Sleep mode offloads model weights to CPU and discards KV cache to free GPU memory.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SleepableEngineMixin(Protocol):
    """Protocol for engines that support sleep/wakeup.

    Sleep mode offloads model weights to CPU RAM and discards the KV cache,
    freeing GPU memory. This is useful for RL training workflows where
    engines need to release GPU memory during training and restore it
    for inference rollouts.
    """

    async def sleep(self, **kwargs: Any) -> None:
        """Put the engine to sleep.

        The caller should guarantee that no requests are being processed
        during the sleep period, before `wakeup` is called.

        Args:
            **kwargs: Engine-specific sleep options. See the concrete engine
                implementation for available options.
        """

    async def wakeup(self, **kwargs: Any) -> None:
        """Wake up the engine from sleep mode.

        Args:
            **kwargs: Engine-specific wakeup options. See the concrete engine
                implementation for available options.
        """

    async def is_sleeping(self) -> bool:
        """Check whether the engine is currently sleeping.

        Returns:
            True if the engine is sleeping, False otherwise.
        """
