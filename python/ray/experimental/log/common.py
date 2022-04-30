from dataclasses import dataclass


@dataclass(init=True)
class NodeIdentifiers:
    node_id: str = None
    node_ip: str = None


@dataclass(init=True)
class FileIdentifiers:
    log_file_name: str = None
    actor_id: str = None
    task_id: str = None
    pid: str = None


@dataclass(init=True)
class LogIdentifiers:
    file: FileIdentifiers
    node: NodeIdentifiers


@dataclass(init=True)
class LogStreamOptions:
    # One of {file, stream}
    media_type: str = None
    lines: int = None
    interval: float = None
