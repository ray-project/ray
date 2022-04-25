import ray

ray.init("auto", namespace="gpu-test")
ray.autoscaler.sdk.request_resources(num_cpus=0)
gpu_actor = ray.get_actor("gpu_actor")
ray.kill(gpu_actor)
