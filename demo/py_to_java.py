import ray
ray.init(job_config=ray.job_config.JobConfig(code_search_path=["."]))
CLASS_NAME = "io.ray.runtime.serializer.ProtobufSerializer$TestActor"

from ray.core.generated.runtime_env_common_pb2 import RuntimeEnv

e = RuntimeEnv()
e.working_dir = "/tmp/ray"

c = ray.java_actor_class(CLASS_NAME)
java_actor = c.remote()
ref = java_actor.returnWorkingDir.remote(e)
print(ray.get(ref))
