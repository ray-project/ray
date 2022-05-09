import ray
import numpy as np


@ray.remote
def f(arr):
    # arr = arr.copy()  # Adding a copy will fix the error.
    arr[0] = 1


ray.get(f.remote(np.zeros(100)))
# ray.exceptions.RayTaskError(ValueError): ray::f()
#   File "test.py", line 6, in f
#     arr[0] = 1
# ValueError: assignment destination is read-only
