import logging
import pathlib
import json
import random
import string

from datetime import datetime

class EventLogger:
    def __init__(self, source: str, dir_path: str):
        self.logger = logging.getLogger("_ray_event_logger")
        self.logger.setLevel(logging.INFO)
        dir_path = pathlib.Path(dir_path) / "events"
        filepath = dir_path/ f"event_{source}.log"
        dir_path.mkdir(exist_ok=True)
        filepath.touch(exist_ok=True)
        # Configure the logger.
        handler = logging.FileHandler(filepath)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def emit(self, *, type: str, message: str, **kwargs):
        self.logger.info(json.dumps({
            "event_id": "".join([random.choice(string.hexdigits) for _ in range(36)]),
            "timestamp": datetime.now().timestamp(),
            "type": type,
            "message": message,
            **kwargs,
        }))
        # Force flush so that we won't lose events
        self.logger.handlers[0].flush()
