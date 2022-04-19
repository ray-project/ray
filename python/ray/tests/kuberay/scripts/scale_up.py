import ray

ray.init("auto")
ray.autoscaler.sdk.request_resources(num_cpus=2)
