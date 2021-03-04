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
        runtime_env (dict): A dict used to specify runtime environment of
            the job. There are three important fields.
            - `working_dir (str)`: A string of working directory
            - `working_dir_uri (str)`: A string of uri of the package stored.
            - `local_modules (list[module])`: List of python modules.
            `working_dir` and `working_dir_uri` are used for the same thing.
            `working_dir_uri` will be used with higher priority. This field
            is used for environment managed by user.
    """

    def __init__(self,
                 worker_env=None,
                 num_java_workers_per_process=1,
                 jvm_options=None,
                 code_search_path=None,
                 runtime_env=None):
        self.worker_env = worker_env or dict()
        self.num_java_workers_per_process = num_java_workers_per_process
        self.jvm_options = jvm_options or []
        self.code_search_path = code_search_path or []
        self.runtime_env = runtime_env or dict()

    def serialize(self):
        job_config = ray.gcs_utils.JobConfig()
        for key in self.worker_env:
            job_config.worker_env[key] = self.worker_env[key]
        job_config.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_config.jvm_options.extend(self.jvm_options)
        job_config.code_search_path.extend(self.code_search_path)
        job_config.runtime_env = self._get_proto_runtime()
        return job_config.SerializeToString()

    def _get_proto_runtime(self):
        from ray.core.generated.common_pb2 import RuntimeEnv
        runtime_env = RuntimeEnv()
        if self.runtime_env.get("working_dir_uri"):
            runtime_env.working_dir_urk = self.runtime_env.get(
                "working_dir_uri")
