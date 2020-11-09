import ray

from ray.experimental.client.api import APIImpl


class CoreRayAPI(APIImpl):
    def get(self, *args, **kwargs):
        return ray.get(*args, **kwargs)

    def put(self, *args, **kwargs):
        return ray.put(*args, **kwargs)

    def remote(self, *args, **kwargs):
        return ray.remote(*args, **kwargs)


def set_client_api_as_ray():
    ray_api = CoreRayAPI()
    ray.experimental.client._set_client_api(ray_api)
