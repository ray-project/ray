import tarfile, io
from typing import List
import PIL.Image
import numpy as np
import boto3
import ray

s3 = boto3.client("s3")

def load_chunk(tarfile, size=None):
  """Load a number of images from a single imagenet .tar file.

  This function also converts the image from grayscale to RGB if neccessary.

  Args:
    tarfile (tarfile.TarFile): The archive from which the files get loaded.
    size (Optional[Tuple[int, int]]): Resize the image to this size if provided.

  Returns:
    numpy.ndarray: Contains the image data in format [batch, w, h, c]
  """

  result = []
  for member in tarfile.getmembers():
    filename = member.path
    content = tarfile.extractfile(member)
    img = PIL.Image.open(content)
    rgbimg = PIL.Image.new("RGB", img.size)
    rgbimg.paste(img)
    if size != None:
      rgbimg = rgbimg.resize(size, PIL.Image.ANTIALIAS)
    result.append(np.array(rgbimg).reshape(1, rgbimg.size[0], rgbimg.size[1], 3))
  return np.concatenate(result)

@ray.remote([str, str, List[int]], [np.ndarray])
def load_tarfile_from_s3(bucket, s3_key, size=[]):
  """Load an imagenet .tar file.

  Args:
    bucket (str): Bucket holding the imagenet .tar.
    s3_key (str): s3 key from which the .tar file is loaded.
    size (List[int]): Resize the image to this size if size != []; len(size) == 2 required.

  Returns:
    np.ndarray: The image data (see load_chunk).
  """

  response = s3.get_object(Bucket=bucket, Key=s3_key)
  output = io.BytesIO()
  chunk = response["Body"].read(1024 * 8)
  while chunk:
    output.write(chunk)
    chunk = response["Body"].read(1024 * 8)
  output.seek(0) # go to the beginning of the .tar file
  tar = tarfile.open(mode="r", fileobj=output)
  return load_chunk(tar, size=size if size != [] else None)

@ray.remote([str, List[str], List[int]], [List[ray.ObjRef]])
def load_tarfiles_from_s3(bucket, s3_keys, size=[]):
  """Load a number of imagenet .tar files.

  Args:
    bucket (str): Bucket holding the imagenet .tars.
    s3_keys (List[str]): List of s3 keys from which the .tar files are being loaded.
    size (List[int]): Resize the image to this size if size != []; len(size) == 2 required.

  Returns:
    np.ndarray: Contains object references to the chunks of the images (see load_chunk).
  """

  return [load_tarfile_from_s3(bucket, s3_key, size) for s3_key in s3_keys]
