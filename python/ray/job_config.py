import ray
from ray.utils import get_conda_env_dir


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
        runtime_env (dict): Specifies environment for worker processes.
            Supports the key "conda_env" which maps to the name (str) of an
            existing conda env the worker process should start in.
        worker_env (dict): Environment variables to be set on worker
            processes.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        jvm_options (str[]): The jvm options for java workers of the job.
        code_search_path (list): A list of directories or jar files that
            specify the search path for user code. This will be used as
            `CLASSPATH` in Java and `PYTHONPATH` in Python.
    """

    def __init__(
            self,
            runtime_env=None,
            worker_env=None,
            num_java_workers_per_process=1,
            jvm_options=None,
            code_search_path=None,
    ):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        if runtime_env:
            conda_env = runtime_env.get("conda_env")
            if conda_env is not None:
                conda_env_dir = get_conda_env_dir(conda_env)
                if self.worker_env.get("PYTHONHOME") is not None:
                    raise ValueError(
                        f"worker_env specifies PYTHONHOME="
                        f"{self.worker_env['PYTHONHOME']} which "
                        f"conflicts with PYTHONHOME={conda_env_dir} "
                        f"required by the specified conda env "
                        f"{runtime_env['conda_env']}.")
                self.worker_env.update(PYTHONHOME=conda_env_dir)
        self.num_java_workers_per_process = num_java_workers_per_process
        if jvm_options is None:
            self.jvm_options = []
        else:
            self.jvm_options = jvm_options
        if code_search_path is None:
            self.code_search_path = []
        else:
            self.code_search_path = code_search_path

    def serialize(self):
        job_config = ray.gcs_utils.JobConfig()
        for key in self.worker_env:
            job_config.worker_env[key] = self.worker_env[key]
        job_config.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_config.jvm_options.extend(self.jvm_options)
        job_config.code_search_path.extend(self.code_search_path)
        return job_config.SerializeToString()
