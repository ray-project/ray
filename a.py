import time
import ray
import pickle
from ray._private.internal_api import memory_summary
import ray.exceptions


@ray.remote(num_cpus=0.1)
def out_of_band_serialization_pickle():
    obj_ref = ray.put(1)
    import pickle
    # object_ref is serialized from user code using a regular pickle.
    # Ray cannot keep track of the reference, so the underlying object
    # can be GC'ed unexpectedly which can cause unexpected hangs.
    return pickle.dumps(obj_ref)

@ray.remote(num_cpus=0.1)
def out_of_band_serialization_ray_cloudpickle():
    from ray import cloudpickle
    obj_ref = ray.put(1)
    return cloudpickle.dumps(obj_ref)


result = ray.get(out_of_band_serialization_pickle.remote())
try:
    ray.get(pickle.loads(result), timeout=5)
except ray.exceptions.GetTimeoutError:
    print("Underlying object is unexpectedly GC'ed!")

# By default, it is not allowed to serialize ray.ObjectRef using
# ray.cloudpickle.
try:
    ray.get(out_of_band_serialization_ray_cloudpickle.remote())
except Exception as e:
    print(f"Exception raised from out_of_band_serialization_ray_cloudpickle {e}")

# It is allowed to use ray.cloudpickle to serialize object ref using an env var.
ray.get(out_of_band_serialization_pickle.options(runtime_env={"env_vars": {
    "RAY_allow_out_of_band_object_ref_serialization": "1"
}}).remote())
# Wait long enough to make sure the task is finished.
time.sleep(5)
# you can see objects are stil pinned although it is GC'ed and not used anymore.
print(memory_summary())
