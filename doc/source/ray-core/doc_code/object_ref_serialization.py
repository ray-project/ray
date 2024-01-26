import ray
from ray import cloudpickle

FILE = "external_store.pickle"

ray.init()

my_dict = {"hello": "world"}

obj_ref = ray.put(my_dict)
with open(FILE, "wb+") as f:
    cloudpickle.dump(obj_ref, f)

# Object ref remains pinned in memory because
# it was serialized with ray.cloudpickle.
del obj_ref

with open(FILE, "rb") as f:
    new_obj_ref = cloudpickle.load(f)

# The deserialized object ref works as expected.
assert ray.get(new_obj_ref) == my_dict

# Explicitly free the object.
ray._private.internal_api.free(new_obj_ref)
