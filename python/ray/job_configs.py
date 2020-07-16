import ray


class JobConfigs:
    """A class used to store the configurations of a job.

    Attributes:
        num_initial_python_workers (int): The initial Python workers to start
            per node. If a negative value is specified, it fallbacks to
            `num_cpus`.
        num_initial_java_workers (int): The initial Java workers to start per
            node. If a negative value is specified, it fallbacks to
            `num_cpus`.
        worker_env (dict): Environment variables to be set on worker
            processes.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        jvm_options (str[]): The jvm options for java workers of the job.
    """

    def __init__(
            self,
            num_initial_python_workers=-1,
            num_initial_java_workers=-1,
            worker_env=None,
            num_java_workers_per_process=10,
            jvm_options=None,
    ):
        self.num_initial_python_workers = num_initial_python_workers
        self.num_initial_java_workers = num_initial_java_workers
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        self.num_java_workers_per_process = num_java_workers_per_process
        if jvm_options is None:
            self.jvm_options = []
        else:
            self.jvm_options = jvm_options

    def serialize(self):
        job_configs = ray.gcs_utils.JobConfigs()
        job_configs.num_initial_python_workers = (
            self.num_initial_python_workers)
        job_configs.num_initial_java_workers = self.num_initial_java_workers
        for key in self.worker_env:
            job_configs.worker_env[key] = self.worker_env[key]
        job_configs.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_configs.jvm_options.extend(self.jvm_options)
        return job_configs.SerializeToString()
