import ray

if __name__ == "__main__":
    ray.init("auto")
    ray.autoscaler.sdk.request_resources(num_cpus=2)
