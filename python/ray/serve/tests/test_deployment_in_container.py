import sys

import numpy as np
import pytest

import ray
from ray.serve.handle import DeploymentHandle


def check_application(app_handle: DeploymentHandle, expected: str):
    ref = app_handle.remote()
    assert ref.result() == expected
    return True


# @pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
# def test_basic(ray_start_stop):
#     @serve.deployment(
#         ray_actor_options={
#             "runtime_env": {
#                 "container": {
#                     "image": "zcin/runtime-env-prototype:nested",
#                     "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
#                 }
#             }
#         }
#     )
#     class Model:
#         def __call__(self):
#             with open("file.txt") as f:
#                 return f.read()

#     h = serve.run(Model.bind())
#     wait_for_condition(
#         check_application,
#         app_handle=h,
#         expected="Hi I'm Cindy, this is version 1\n",
#         timeout=300,
#     )


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_put_get(ray_start_stop):
    # ray.init(
    # )
    # refs = []
    # for _ in range(10):
    #     refs.append(ray.put(bytearray(20 * 1024 * 1024)))

    # @ray.remote
    # def consume(refs):
    #     # Should work without releasing resources!
    #     ray.get(refs)
    #     return os.getpid()

    # ref = consume.remote([ray.put(909)])
    # ray.get(ref)
    @ray.remote(
        runtime_env={
            "container": {
                "image": "zcin/ray:nightly-py3816-cpu",
                "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
            }
        }
    )
    def create_ref():
        ref = ray.put(np.zeros(100_000_000))
        return ref

    wrapped_ref = create_ref.remote()
    print(ray.get(ray.get(wrapped_ref)))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
