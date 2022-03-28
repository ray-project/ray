from typing import Any, Dict, Optional, Union
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
        default_actor_lifetime (str): The default value of actor lifetime.
    """

    def __init__(
        self,
        num_java_workers_per_process=1,
        jvm_options=None,
        code_search_path=None,
        runtime_env=None,
        client_job=False,
        metadata=None,
        ray_namespace=None,
        default_actor_lifetime="non_detached",
    ):
        self.num_java_workers_per_process = num_java_workers_per_process
        self.jvm_options = jvm_options or []
        self.code_search_path = code_search_path or []
        # It's difficult to find the error that caused by the
        # code_search_path is a string. So we assert here.
        assert isinstance(self.code_search_path, (list, tuple)), (
            f"The type of code search path is incorrect: " f"{type(code_search_path)}"
        )
        self.client_job = client_job
        self.metadata = metadata or {}
        self.ray_namespace = ray_namespace
        self.set_runtime_env(runtime_env)
        self.set_default_actor_lifetime(default_actor_lifetime)

    def set_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def serialize(self):
        """Serialize the struct into protobuf string"""
        return self.get_proto_job_config().SerializeToString()

    def set_runtime_env(
        self,
        runtime_env: Optional[Union[Dict[str, Any], "RuntimeEnv"]],  # noqa: F821
        validate: bool = False,
    ) -> None:
        """Modify the runtime_env of the JobConfig.

        We don't validate the runtime_env by default here because it may go
        through some translation before actually being passed to C++ (e.g.,
        working_dir translated from a local directory to a URI.
        """
        self.runtime_env = runtime_env if runtime_env is not None else {}
        if validate:
            self.runtime_env = self._validate_runtime_env()
        self._cached_pb = None

    def set_ray_namespace(self, ray_namespace: str) -> None:
        if ray_namespace != self.ray_namespace:
            self.ray_namespace = ray_namespace
            self._cached_pb = None

    def set_default_actor_lifetime(self, default_actor_lifetime: str) -> None:
        if default_actor_lifetime == "detached":
            self._default_actor_lifetime = gcs_utils.JobConfig.ActorLifetime.DETACHED
        elif default_actor_lifetime == "non_detached":
            self._default_actor_lifetime = (
                gcs_utils.JobConfig.ActorLifetime.NON_DETACHED
            )
        else:
            raise ValueError(
                "Default actor lifetime must be one of `detached`, `non_detached`"
            )

    def _validate_runtime_env(self):
        # TODO(edoakes): this is really unfortunate, but JobConfig is imported
        # all over the place so this causes circular imports. We should remove
        # this dependency and pass in a validated runtime_env instead.
        from ray.runtime_env import RuntimeEnv

        if isinstance(self.runtime_env, RuntimeEnv):
            return self.runtime_env
        return RuntimeEnv(**self.runtime_env)

    def get_proto_job_config(self):
        """Return the protobuf structure of JobConfig."""
        # TODO(edoakes): this is really unfortunate, but JobConfig is imported
        # all over the place so this causes circular imports. We should remove
        # this dependency and pass in a validated runtime_env instead.
        from ray.utils import get_runtime_env_info

        if self._cached_pb is None:
            pb = gcs_utils.JobConfig()
            if self.ray_namespace is None:
                pb.ray_namespace = str(uuid.uuid4())
            else:
                pb.ray_namespace = self.ray_namespace
            pb.num_java_workers_per_process = self.num_java_workers_per_process
            pb.jvm_options.extend(self.jvm_options)
            pb.code_search_path.extend(self.code_search_path)
            for k, v in self.metadata.items():
                pb.metadata[k] = v

            parsed_env = self._validate_runtime_env()
            pb.runtime_env_info.CopyFrom(
                get_runtime_env_info(
                    parsed_env,
                    is_job_runtime_env=True,
                    serialize=False,
                )
            )

            if self._default_actor_lifetime is not None:
                pb.default_actor_lifetime = self._default_actor_lifetime
            self._cached_pb = pb

        return self._cached_pb

    def runtime_env_has_uris(self):
        """Whether there are uris in runtime env or not"""
        return self._validate_runtime_env().has_uris()

    def get_serialized_runtime_env(self) -> str:
        """Return the JSON-serialized parsed runtime env dict"""
        return self._validate_runtime_env().serialize()

    def get_proto_runtime_env_config(self) -> str:
        """Return the JSON-serialized parsed runtime env info"""
        return self.get_proto_job_config().runtime_env_info.runtime_env_config

    @classmethod
    def from_json(cls, job_config_json):
        """
        Generates a JobConfig object from json.
        """
        return cls(
            num_java_workers_per_process=job_config_json.get(
                "num_java_workers_per_process", 1
            ),
            jvm_options=job_config_json.get("jvm_options", None),
            code_search_path=job_config_json.get("code_search_path", None),
            runtime_env=job_config_json.get("runtime_env", None),
            client_job=job_config_json.get("client_job", False),
            metadata=job_config_json.get("metadata", None),
            ray_namespace=job_config_json.get("ray_namespace", None),
        )
