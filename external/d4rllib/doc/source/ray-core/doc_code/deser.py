import ray
import numpy as np


@ray.remote
def f(arr):
    # arr = arr.copy()  # Adding a copy will fix the error.
    arr[0] = 1


try:
    ray.get(f.remote(np.zeros(100)))
except ray.exceptions.RayTaskError as e:
    print(e)
# ray.exceptions.RayTaskError(ValueError): ray::f()
#   File "test.py", line 6, in f
#     arr[0] = 1
# ValueError: assignment destination is read-only
