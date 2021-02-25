import ray


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
        worker_env (dict): Environment variables to be set on worker
            processes.
        worker_cwd (str): The current working directory of worker.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        jvm_options (str[]): The jvm options for java workers of the job.
        code_search_path (list): A list of directories or jar files that
            specify the search path for user code. This will be used as
            `CLASSPATH` in Java and `PYTHONPATH` in Python.
        python_worker_executable (str): The executable path for Python worker.
    """

    def __init__(
            self,
            *,
            worker_env=None,
            worker_cwd=None,
            num_java_workers_per_process=1,
            jvm_options=None,
            code_search_path=None,
            python_worker_executable=None,
            is_submitted_from_dashboard=False,
    ):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        self.worker_cwd = worker_cwd or ""
        self.num_java_workers_per_process = num_java_workers_per_process
        if jvm_options is None:
            self.jvm_options = []
        else:
            self.jvm_options = jvm_options
        if code_search_path is None:
            self.code_search_path = []
        else:
            self.code_search_path = code_search_path
        self.python_worker_executable = python_worker_executable or ""
        self.is_submitted_from_dashboard = is_submitted_from_dashboard

    def serialize(self):
        job_config = ray.gcs_utils.JobConfig()
        for key in self.worker_env:
            job_config.worker_env[key] = self.worker_env[key]
        job_config.worker_cwd = self.worker_cwd
        job_config.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_config.jvm_options.extend(self.jvm_options)
        job_config.code_search_path.extend(self.code_search_path)
        job_config.python_worker_executable = self.python_worker_executable
        job_config.is_submitted_from_dashboard = \
            self.is_submitted_from_dashboard
        return job_config.SerializeToString()
