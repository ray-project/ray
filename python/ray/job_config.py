import ray


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
                 namespace=None):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        import ray._private.runtime_env as runtime_support
        if runtime_env:
            # Remove working_dir rom the dict here, since that needs to be
            # uploaded to the GCS after the job starts.
            without_dir = dict(runtime_env)
            if "working_dir" in without_dir:
                del without_dir["working_dir"]
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict(
                without_dir)
            self.worker_env = self._parsed_runtime_env.to_worker_env_vars(
                self.worker_env)
        else:
            self._parsed_runtime_env = runtime_support.RuntimeEnvDict({})
        self.num_java_workers_per_process = num_java_workers_per_process
        self.jvm_options = jvm_options or []
        self.code_search_path = code_search_path or []
        # It's difficult to find the error that caused by the
        # code_search_path is a string. So we assert here.
        assert isinstance(self.code_search_path, (list, tuple)), \
            f"The type of code search path is incorrect: " \
            f"{type(code_search_path)}"
        self.runtime_env = runtime_env or dict()
        self.client_job = client_job
        self.ray_namespace = namespace

    def serialize(self):
        """Serialize the struct into protobuf string"""
        job_config = self.get_proto_job_config()
        return job_config.SerializeToString()

    def get_proto_job_config(self):
        """Return the prototype structure of JobConfig"""
        job_config = ray.gcs_utils.JobConfig()
        # TODO (alex): should the default here be a uuid?
        job_config.ray_namespace = self.ray_namespace or ""
        for key in self.worker_env:
            job_config.worker_env[key] = self.worker_env[key]
        job_config.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_config.jvm_options.extend(self.jvm_options)
        job_config.code_search_path.extend(self.code_search_path)
        job_config.runtime_env.CopyFrom(self._get_proto_runtime())
        return job_config

    def get_runtime_env_uris(self):
        """Get the uris of runtime environment"""
        if self.runtime_env.get("uris"):
            return self.runtime_env.get("uris")
        return []

    def _get_proto_runtime(self):
        from ray.core.generated.common_pb2 import RuntimeEnv
        runtime_env = RuntimeEnv()
        runtime_env.uris[:] = self.get_runtime_env_uris()
        runtime_env.conda_env_name = (self._parsed_runtime_env.conda_env_name
                                      or "")
        return runtime_env
