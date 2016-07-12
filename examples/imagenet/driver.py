import argparse
import boto3
import os
import numpy as np
import ray
import imagenet

import functions

parser = argparse.ArgumentParser(description="Parse information for data loading.")
parser.add_argument("--s3-bucket", type=str, required=True, help="Name of the bucket that contains the image data.")
parser.add_argument("--key-prefix", default="ILSVRC2012_img_train/n015", type=str, help="Prefix for files to fetch.")
parser.add_argument("--drop-ipython", default=False, type=bool, help="Drop into IPython at the end?")

if __name__ == "__main__":
  args = parser.parse_args()
  worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")
  ray.services.start_ray_local(num_workers=5, worker_path=worker_path)

  s3 = boto3.resource("s3")
  imagenet_bucket = s3.Bucket(args.s3_bucket)
  objects = imagenet_bucket.objects.filter(Prefix=args.key_prefix)
  images = [obj.key for obj in objects.all()]

  x = imagenet.load_tarfiles_from_s3(args.s3_bucket, map(str, images), [256, 256]) # TODO(pcm): implement unicode serialization

  mean_image = functions.compute_mean_image(x)
  mean_image = ray.get(mean_image)

  print "The mean image is:"
  print mean_image

  if args.drop_ipython:
    import IPython
    IPython.embed()
