import logging

def _in_worker_process() -> bool:
    import ray

    return (
        hasattr(ray, "_private")
        and hasattr(ray._private, "worker")
        and ray._private.worker.global_worker.mode
        == ray._private.worker.WORKER_MODE
    )

class PlainRayHandler(logging.StreamHandler):
    """A plain log handler.

    This handler writes to whatever sys.stderr points to at emit-time,
    not at instantiation time. See docs for logging._StderrHandler.
    """

    def __init__(self):
        super().__init__()
        self.plain_handler = logging._StderrHandler()
        self.plain_handler.level = self.level
        self.plain_handler.formatter = logging.Formatter(fmt="%(message)s")

    def emit(self, record: logging.LogRecord):
        """Emit the log message.

        If this is a worker, bypass fancy logging and just emit the log record.
        If this is the driver, emit the message using the appropriate console handler.

        Args:
            record: Log record to be emitted
        """
        if _in_worker_process():
            self.plain_handler.emit(record)
        else:
            logging._StderrHandler.emit(self, record)
