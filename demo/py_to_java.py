from ray.core.generated.runtime_env_common_pb2 import RuntimeEnv
import ray

ray.init(job_config=ray.job_config.JobConfig(code_search_path=["."]))

e = RuntimeEnv()
e.working_dir = "/tmp/simon"

c = ray.java_actor_class(
    "io.ray.runtime.serializer.ProtobufSerializer$TestActor")
java_actor = c.remote()
ref = java_actor.returnWorkingDir.remote(e)
print("-------")
print(ray.get(ref))
