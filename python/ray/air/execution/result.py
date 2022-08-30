from dataclasses import dataclass

from ray.air.execution.resources.request import ResourceRequest


@dataclass
class ExecutionResult:
    pass


@dataclass
class ExecutionException(ExecutionResult):
    exception: Exception


@dataclass
class ResourceResult(ExecutionResult):
    resource_request: ResourceRequest
