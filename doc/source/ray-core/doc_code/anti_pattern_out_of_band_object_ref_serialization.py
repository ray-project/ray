# __anti_pattern_start__
import ray
import pickle
from ray._private.internal_api import memory_summary
import ray.exceptions

ray.init()


@ray.remote
def out_of_band_serialization_pickle():
    obj_ref = ray.put(1)
    import pickle

    # object_ref is serialized from user code using a regular pickle.
    # Ray can't keep track of the reference, so the underlying object
    # can be GC'ed unexpectedly, which can cause unexpected hangs.
    return pickle.dumps(obj_ref)


@ray.remote
def out_of_band_serialization_ray_cloudpickle():
    obj_ref = ray.put(1)
    from ray import cloudpickle

    # ray.cloudpickle can serialize only when
    # RAY_allow_out_of_band_object_ref_serialization=1 env var is set.
    # However, the object_ref is pinned for the lifetime of the worker,
    # which can cause Ray object leaks that can cause spilling.
    return cloudpickle.dumps(obj_ref)


print("==== serialize object ref with pickle ====")
result = ray.get(out_of_band_serialization_pickle.remote())
try:
    ray.get(pickle.loads(result), timeout=5)
except ray.exceptions.GetTimeoutError:
    print("Underlying object is unexpectedly GC'ed!\n\n")

print("==== serialize object ref with ray.cloudpickle ====")
# By default, it's allowed to serialize ray.ObjectRef using
# ray.cloudpickle.
ray.get(out_of_band_serialization_ray_cloudpickle.options().remote())
# you can see objects are stil pinned although it's GC'ed and not used anymore.
print(memory_summary())

print(
    "==== serialize object ref with ray.cloudpickle with env var "
    "RAY_allow_out_of_band_object_ref_serialization=0 for debugging ===="
)
try:
    ray.get(
        out_of_band_serialization_ray_cloudpickle.options(
            runtime_env={
                "env_vars": {
                    "RAY_allow_out_of_band_object_ref_serialization": "0",
                }
            }
        ).remote()
    )
except Exception as e:
    print(f"Exception raised from out_of_band_serialization_ray_cloudpickle {e}\n\n")

# __anti_pattern_end__
