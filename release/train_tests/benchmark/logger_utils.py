import logging
import inspect


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        frame = inspect.currentframe().f_back.f_back  # Go two frames back
        class_name = getattr(frame.f_locals.get("self"), "__class__", None)
        func_name = frame.f_code.co_name
        prefix = (
            f"[{class_name.__name__}/{func_name}]" if class_name else f"[{func_name}]"
        )
        return f"{prefix} {msg}", kwargs
