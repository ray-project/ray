from typing import Any, Dict, Optional
import uuid

import ray._private.gcs_utils as gcs_utils


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
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
                 num_java_workers_per_process=1,
                 jvm_options=None,
                 code_search_path=None,
                 runtime_env=None,
                 client_job=False,
                 metadata=None,
                 ray_namespace=None):
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
        return self.get_proto_job_config().SerializeToString()

    def set_runtime_env(self, runtime_env: Optional[Dict[str, Any]]) -> None:
        # TODO(edoakes): this is really unfortunate, but JobConfig is imported
        # all over the place so this causes circular imports. We should remove
        # this dependency and pass in a validated runtime_env instead.
        from ray._private.runtime_env.validation import ParsedRuntimeEnv
        self._parsed_runtime_env = ParsedRuntimeEnv(runtime_env or {})
        self.runtime_env = runtime_env or dict()
        eager_install = False
        if runtime_env and "eager_install" in runtime_env:
            eager_install = runtime_env["eager_install"]
        self.runtime_env_eager_install = eager_install
        assert isinstance(self.runtime_env_eager_install, bool), \
            f"The type of eager_install is incorrect: " \
            f"{type(self.runtime_env_eager_install)}" \
            f", the bool type is needed."
        self._cached_pb = None

    def set_ray_namespace(self, ray_namespace: str) -> None:
        if ray_namespace != self.ray_namespace:
            self.ray_namespace = ray_namespace
            self._cached_pb = None

    def get_proto_job_config(self):
        """Return the prototype structure of JobConfig"""
        if self._cached_pb is None:
            self._cached_pb = gcs_utils.JobConfig()
            if self.ray_namespace is None:
                self._cached_pb.ray_namespace = str(uuid.uuid4())
            else:
                self._cached_pb.ray_namespace = self.ray_namespace
            self._cached_pb.num_java_workers_per_process = (
                self.num_java_workers_per_process)
            self._cached_pb.jvm_options.extend(self.jvm_options)
            self._cached_pb.code_search_path.extend(self.code_search_path)
            self._cached_pb.runtime_env.uris[:] = self.get_runtime_env_uris()
            serialized_env = self.get_serialized_runtime_env()
            self._cached_pb.runtime_env.serialized_runtime_env = serialized_env
            for k, v in self.metadata.items():
                self._cached_pb.metadata[k] = v
            self._cached_pb.runtime_env.runtime_env_eager_install = \
                self.runtime_env_eager_install
        return self._cached_pb

    def get_runtime_env_uris(self):
        """Get the uris of runtime environment"""
        return self._parsed_runtime_env.get("uris") or []

    def get_serialized_runtime_env(self) -> str:
        """Return the JSON-serialized parsed runtime env dict"""
        return self._parsed_runtime_env.serialize()

    def set_runtime_env_uris(self, uris):
        self.runtime_env["uris"] = uris
        self._parsed_runtime_env["uris"] = uris
