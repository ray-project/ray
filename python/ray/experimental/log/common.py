from dataclasses import dataclass


@dataclass(init=True)
class FileIdentifiers:
    log_file_name: str = None
    actor_id: str = None
    task_id: str = None
    pid: str = None


# SANG-TODO
@dataclass(init=True)
class LogStreamOptions:
    # One of {file, stream}
    media_type: str
    lines: int = 1000
    interval: float = None
