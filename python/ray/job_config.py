import ray
from typing import Any, Dict, Optional
import uuid
import json

from ray.core.generated.common_pb2 import RuntimeEnv as RuntimeEnvPB


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
        worker_env (dict): Environment variables to be set on worker
            processes.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        jvm_options (str[]): The jvm options for java workers of the job.
        code_search_path (list): A list of directories or jar files that
            specify the search path for user code. This will be used as
            `CLASSPATH` in Java and `PYTHONPATH` in Python.
        runtime_env (dict): A runtime environment dictionary (see
            ``runtime_env.py`` for detailed documentation).
        client_job (bool): A boolean represent the source of the job.
    """

    def __init__(self,
                 worker_env=None,
                 num_java_workers_per_process=1,
                 jvm_options=None,
                 code_search_path=None,
                 runtime_env=None,
                 client_job=False,
                 metadata=None,
                 ray_namespace=None):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        self.num_java_workers_per_process = num_java_workers_per_process
        self.jvm_options = jvm_options or []
        self.code_search_path = code_search_path or []
        # It's difficult to find the error that caused by the
        # code_search_path is a string. So we assert here.
        assert isinstance(self.code_search_path, (list, tuple)), \
            f"The type of code search path is incorrect: " \
            f"{type(code_search_path)}"
        self.client_job = client_job
        self.metadata = metadata or {}
        self.ray_namespace = ray_namespace
        self.set_runtime_env(runtime_env)

    def set_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def serialize(self):
        """Serialize the struct into protobuf string"""
        job_config = self.get_proto_job_config()
        return job_config.SerializeToString()

    def set_runtime_env(self, runtime_env: Optional[Dict[str, Any]]) -> None:
        # Lazily import this to avoid circular dependencies.
        import ray._private.runtime_env as runtime_support
        if runtime_env:
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict(
                runtime_env)
            self.worker_env.update(
                self._parsed_runtime_env.get_parsed_dict().get("env_vars")
                or {})
        else:
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict({})
        self.runtime_env = runtime_env or dict()
        self._cached_pb = None

    def set_ray_namespace(self, ray_namespace: str) -> None:
        if ray_namespace != self.ray_namespace:
            self.ray_namespace = ray_namespace
            self._cached_pb = None

    def get_proto_job_config(self):
        """Return the prototype structure of JobConfig"""
        if self._cached_pb is None:
            self._cached_pb = ray.gcs_utils.JobConfig()
            if self.ray_namespace is None:
                self._cached_pb.ray_namespace = str(uuid.uuid4())
            else:
                self._cached_pb.ray_namespace = self.ray_namespace
            for key in self.worker_env:
                self._cached_pb.worker_env[key] = self.worker_env[key]
            self._cached_pb.num_java_workers_per_process = (
                self.num_java_workers_per_process)
            self._cached_pb.jvm_options.extend(self.jvm_options)
            self._cached_pb.code_search_path.extend(self.code_search_path)
            self._cached_pb.runtime_env.CopyFrom(self._get_proto_runtime())
            self._cached_pb.serialized_runtime_env = \
                self.get_serialized_runtime_env()
            for k, v in self.metadata.items():
                self._cached_pb.metadata[k] = v
        return self._cached_pb

    def get_runtime_env_uris(self):
        """Get the uris of runtime environment"""
        if self.runtime_env.get("uris"):
            return self.runtime_env.get("uris")
        return []

    def set_runtime_env_uris(self, uris):
        self.runtime_env["uris"] = uris
        self._parsed_runtime_env.set_uris(uris)

    def get_serialized_runtime_env(self) -> str:
        """Return the JSON-serialized parsed runtime env dict"""
        return self._parsed_runtime_env.serialize()

    def _get_proto_runtime(self) -> RuntimeEnvPB:
        runtime_env = RuntimeEnvPB()
        runtime_env.uris[:] = self.get_runtime_env_uris()
        runtime_env.raw_json = json.dumps(self.runtime_env)
        return runtime_env
