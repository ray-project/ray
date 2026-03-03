import logging
import inspect
from typing import Any, Dict, Tuple, Union


class ContextLoggerAdapter(logging.LoggerAdapter):
    def __init__(
        self, logger: logging.Logger, extra: Union[Dict[str, Any], None] = None
    ) -> None:
        """Initialize the logger adapter.

        Args:
            logger: The logger to wrap
            extra: Extra data to include in log records
        """
        super().__init__(logger, extra or {})

    def process(self, msg: str, kwargs: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Process a log message and add context information.

        Args:
            msg: The log message to process
            kwargs: Additional keyword arguments for logging

        Returns:
            Tuple containing:
                - The processed message with context prefix
                - The original kwargs
        """
        # Get the frame that called the logging method
        # Go up 3 frames: process -> log -> info/error/etc -> actual caller
        frame = inspect.currentframe()
        if (
            frame
            and frame.f_back
            and frame.f_back.f_back
            and frame.f_back.f_back.f_back
        ):
            frame = frame.f_back.f_back.f_back
            class_name = getattr(frame.f_locals.get("self"), "__class__", None)
            func_name = frame.f_code.co_name

            # Create the prefix with class and function context
            prefix = (
                f"[{class_name.__name__}.{func_name}]"
                if class_name
                else f"[{func_name}]"
            )
        else:
            prefix = "[unknown]"

        # Add any extra context from the adapter
        # Don't modify kwargs directly as it causes issues with level handling

        return f"{prefix} {msg}", kwargs
