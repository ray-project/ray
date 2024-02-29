import os
import time
import logging
import random
from PIL import Image, ImageFilter
from typing import Tuple, List
import glob
import numpy as np
import ray
import torch
from torchvision import transforms as T

ray.init(address='auto')

DATA_DIR = os.getcwd() + "/task_images"
BATCHES = [10, 20, 30, 40, 50]
SERIAL_BATCH_TIMES = []
DISTRIBUTED_BATCH_TIMES = []
THUMB_SIZE = (64, 64)
image_list = glob.glob(DATA_DIR+"/*.jpg")
NODE_USER_NAME = "ec2-user"
DATA_IP= "10.0.0.132"

def transform_image(img: object, fetch_image=True, verbose=False):
    """
    This is a deliberate compute intensive image transfromation and tensor operation
    to simulate a compute intensive image processing
    """

    before_shape = img.size

    # Make the image blur with specified intensify
    # Use torchvision transformation to augment the image
    img = img.filter(ImageFilter.GaussianBlur(radius=20))
    augmentor = T.TrivialAugmentWide(num_magnitude_bins=31)
    img = augmentor(img)

    # Convert image to tensor and transpose
    tensor = torch.tensor(np.asarray(img))
    t_tensor = torch.transpose(tensor, 0, 1)

    # compute intensive operations on tensors
    random.seed(42)
    for _ in range(3):
        tensor.pow(3).sum()
        t_tensor.pow(3).sum()
        torch.mul(tensor, random.randint(2, 10))
        torch.mul(t_tensor, random.randint(2, 10))
        torch.mul(tensor, tensor)
        torch.mul(t_tensor, t_tensor)

    # Resize to a thumbnail
    img.thumbnail(THUMB_SIZE)
    after_shape = img.size
    if verbose:
        print(f"augmented: shape:{img.size}| image tensor shape:{tensor.size()} transpose shape:{t_tensor.size()}")

    return before_shape, after_shape


# Define a Ray task to transform, augment and do some compute intensive tasks on an image
@ray.remote(num_cpus=1)
def augment_image_distributed(working_dir, complexity_score, fetch_image):
    img = Image.open(working_dir)
    return transform_image(img, fetch_image=fetch_image)

@ray.remote(num_cpus=1)
def augment_image_distributed_manual(image, complexity_score, fetch_image):
    
    if not os.path.exists(image):
        # remote = True
        # time.sleep(complexity_score / 100000)
        # os.system("rsync --mkpath -a ubuntu@172.31.40.126:%s %s" %(image, image))
        os.system(f"rsync --mkpath -a {NODE_USER_NAME}@{DATA_IP}:{image} {image}")
    
    img = Image.open(image)
    return transform_image(img, fetch_image=fetch_image)

# def run_distributed(img_list_refs:List[object]) ->  List[Tuple[int, float]]:
#     return ray.get([augment_image_distributed.remote(img, False, working_dir=img) for img in img_list_refs])

# Check if dir exists. If so ignore download.
# Just assume we have done from a prior run
# if not os.path.exists(DATA_DIR):
#     os.mkdir(DATA_DIR)
#     print(f"downloading images ...")
#     for url in tqdm.tqdm(t_utils.URLS):
#         t_utils.download_images(url, DATA_DIR)
        
# Place all images into the object store. Since Ray tasks may be disributed 
# across machines, the DATA_DIR may not be present on a worker. However,
#placing them into the Ray distributed objector provides access to any 
# remote task scheduled on Ray worker
    
# images_list_refs = [t_utils.insert_into_object_store(image) for image in image_list]
# images_list_refs[:2]

# Iterate over batches, launching Ray task for each image within the processing
# batch
# for idx in BATCHES:
#     image_batch_list_refs = image_list[:idx]

print(f"\nRunning {len(image_list)} tasks distributed....")

# Run each one serially
start = time.perf_counter()

obj_refs = []
for img in image_list:
    cur_complexity = os.stat(img).st_size
    obj_refs.append(augment_image_distributed.remote(
        working_dir=img, # TODO: Has to explicitly do so
        complexity_score=cur_complexity, 
        fetch_image=False
    ))
    # obj_refs.append(augment_image_distributed_manual.remote(
    #     img,
    #     cur_complexity, 
    #     False
    # ))

distributed_results = ray.get(obj_refs)

end = time.perf_counter()
elapsed = end - start

print(elapsed)

# Keep track of batchs, execution times as a Tuple
# DISTRIBUTED_BATCH_TIMES.append((idx, round(elapsed, 2)))
# print(f"Distributed transformations/computations of {len(image_batch_list_refs)} images: {elapsed:.2f} sec")
