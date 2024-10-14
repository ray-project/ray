from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DoAutoMLReply(_message.Message):
    __slots__ = ["message", "success", "task_id"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    message: str
    success: bool
    task_id: int
    def __init__(self, success: bool = ..., task_id: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...

class DoAutoMLRequest(_message.Message):
    __slots__ = ["data_partition", "data_source", "model_season_lengths", "models"]
    DATA_PARTITION_FIELD_NUMBER: _ClassVar[int]
    DATA_SOURCE_FIELD_NUMBER: _ClassVar[int]
    MODELS_FIELD_NUMBER: _ClassVar[int]
    MODEL_SEASON_LENGTHS_FIELD_NUMBER: _ClassVar[int]
    data_partition: str
    data_source: str
    model_season_lengths: _containers.RepeatedScalarFieldContainer[int]
    models: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, data_source: _Optional[str] = ..., data_partition: _Optional[str] = ..., model_season_lengths: _Optional[_Iterable[int]] = ..., models: _Optional[_Iterable[str]] = ...) -> None: ...

class GetResultReply(_message.Message):
    __slots__ = ["result", "success"]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    result: str
    success: bool
    def __init__(self, success: bool = ..., result: _Optional[str] = ...) -> None: ...

class GetResultRequest(_message.Message):
    __slots__ = ["task_id"]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    def __init__(self, task_id: _Optional[int] = ...) -> None: ...

class RegisterReply(_message.Message):
    __slots__ = ["data_partition", "data_source", "message", "model_season_lengths", "models", "success"]
    DATA_PARTITION_FIELD_NUMBER: _ClassVar[int]
    DATA_SOURCE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    MODELS_FIELD_NUMBER: _ClassVar[int]
    MODEL_SEASON_LENGTHS_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    data_partition: str
    data_source: str
    message: str
    model_season_lengths: _containers.RepeatedScalarFieldContainer[int]
    models: _containers.RepeatedScalarFieldContainer[str]
    success: bool
    def __init__(self, success: bool = ..., data_source: _Optional[str] = ..., data_partition: _Optional[str] = ..., model_season_lengths: _Optional[_Iterable[int]] = ..., models: _Optional[_Iterable[str]] = ..., message: _Optional[str] = ...) -> None: ...

class RegisterRequest(_message.Message):
    __slots__ = ["id", "task_id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    task_id: int
    def __init__(self, id: _Optional[str] = ..., task_id: _Optional[int] = ...) -> None: ...

class ReportResultReply(_message.Message):
    __slots__ = ["message", "success"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    message: str
    success: bool
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class ReportResultRequest(_message.Message):
    __slots__ = ["id", "result", "task_id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    result: str
    task_id: int
    def __init__(self, id: _Optional[str] = ..., task_id: _Optional[int] = ..., result: _Optional[str] = ...) -> None: ...

class WorkerRegisterReply(_message.Message):
    __slots__ = ["success", "worker_task"]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    WORKER_TASK_FIELD_NUMBER: _ClassVar[int]
    success: bool
    worker_task: bytes
    def __init__(self, success: bool = ..., worker_task: _Optional[bytes] = ...) -> None: ...

class WorkerRegisterRequest(_message.Message):
    __slots__ = ["worker_id"]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    def __init__(self, worker_id: _Optional[str] = ...) -> None: ...

class WorkerReportResultReply(_message.Message):
    __slots__ = ["message", "success"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    message: str
    success: bool
    def __init__(self, success: bool = ..., message: _Optional[str] = ...) -> None: ...

class WorkerReportResultRequest(_message.Message):
    __slots__ = ["result", "worker_id"]
    class ResultEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    RESULT_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    result: _containers.ScalarMap[str, float]
    worker_id: str
    def __init__(self, worker_id: _Optional[str] = ..., result: _Optional[_Mapping[str, float]] = ...) -> None: ...
