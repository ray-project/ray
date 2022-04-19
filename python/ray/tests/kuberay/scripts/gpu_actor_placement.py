import ray


@ray.remote(num_gpus=1, num_cpus=1)
class GPUActor:
    def where_am_i(self):
        assert len(ray.get_gpu_ids()) == 1
        return "on-a-gpu-node"


ray.init("auto", namespace="gpu-test")
GPUActor.options(name="gpu_actor", lifetime="detached").remote()
