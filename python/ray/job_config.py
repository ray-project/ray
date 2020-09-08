import ray


class JobConfig:
    """A class used to store the configurations of a job.

    Attributes:
        worker_env (dict): Environment variables to be set on worker
            processes.
        num_java_workers_per_process (int): The number of java workers per
            worker process.
        jvm_options (str[]): The jvm options for java workers of the job.
        resource_path (str[]): The resource path for workers of the job.
    """

    def __init__(
            self,
            worker_env=None,
            num_java_workers_per_process=1,
            jvm_options=None,
            resource_path=None,
    ):
        if worker_env is None:
            self.worker_env = dict()
        else:
            self.worker_env = worker_env
        self.num_java_workers_per_process = num_java_workers_per_process
        if jvm_options is None:
            self.jvm_options = []
        else:
            self.jvm_options = jvm_options
        if resource_path is None:
            self.resource_path = []

    def serialize(self):
        job_config = ray.gcs_utils.JobConfig()
        for key in self.worker_env:
            job_config.worker_env[key] = self.worker_env[key]
        job_config.num_java_workers_per_process = (
            self.num_java_workers_per_process)
        job_config.jvm_options.extend(self.jvm_options)
        job_config.resource_path.extend(self.resource_path)
        return job_config.SerializeToString()
