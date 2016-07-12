import numpy as np
from typing import List, Tuple
import ray
import ray.array.remote as ra

@ray.remote([List[Tuple[ray.ObjRef, ray.ObjRef]]], [int])
def num_images(batches):
  shape_refs = [ra.shape(batch[0]) for batch in batches]
  return sum([ray.get(shape_ref)[0] for shape_ref in shape_refs])

@ray.remote([List[Tuple[ray.ObjRef, ray.ObjRef]]], [np.ndarray])
def compute_mean_image(batches):
  if len(batches) == 0:
    raise Exception("No images were passed into `compute_mean_image`.")
  sum_image_refs = [ra.sum(batch[0], axis=0) for batch in batches]
  sum_images = [ray.get(ref) for ref in sum_image_refs]
  n_images = num_images(batches)
  return np.sum(sum_images, axis=0).astype("float64") / ray.get(n_images)
