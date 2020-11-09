import ray

from ray.experimental.client.api import APIImpl


class CoreRayAPI(APIImpl):
    def get(self, *args, **kwargs):
        return ray.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return ray.put(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return ray.remote(*args, **kwargs)

    def call_remote(self, f, *args, **kwargs):
        return f.remote(*args, **kwargs)

    def close(self, *args, **kwargs):
        return None


def set_client_api_as_ray():
    ray_api = CoreRayAPI()
    ray.experimental.client._set_client_api(ray_api)
