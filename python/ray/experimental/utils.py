from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import io
import os
import tarfile
import sys

import ray


def tarred_directory_as_bytes(source_dir):
  """Tar a directory and return it as a byte string.

  Args:
    source_dir (str): The name of the directory to tar.

  Returns:
    A byte string representing the tarred file.
  """
  # Get a BytesIO object.
  string_file = io.BytesIO()
  # Create an in-memory tarfile of the source directory.
  with tarfile.open(mode="w:gz", fileobj=string_file) as tar:
    tar.add(source_dir, arcname=os.path.basename(source_dir))
  string_file.seek(0)
  return string_file.read()


def tarred_bytes_to_directory(tarred_bytes, target_dir):
  """Take a byte string and untar it.

  Args:
    tarred_bytes (str): A byte string representing the tarred file. This should
      be the output of tarred_directory_as_bytes.
    target_dir (str): The directory to create the untarred files in.
  """
  string_file = io.BytesIO(tarred_bytes)
  with tarfile.open(fileobj=string_file) as tar:
    tar.extractall(path=target_dir)


def copy_directory(source_dir, target_dir=None):
  """Copy a local directory to each machine in the Ray cluster.

  Note that both source_dir and target_dir must have the same basename). For
  example, source_dir can be /a/b/c and target_dir can be /d/e/c. In this case,
  the directory /d/e will be added to the Python path of each worker.

  Note that this method is not completely safe to use. For example, workers
  that do not do the copying and only set their paths (only one worker per node
  does the copying) may try to execute functions that use the files in the
  directory being copied before the directory being copied has finished
  untarring.

  Args:
    source_dir (str): The directory to copy.
    target_dir (str): The location to copy it to on the other machines. If this
      is not provided, the source_dir will be used. If it is provided and is
      different from source_dir, the source_dir also be copied to the
      target_dir location on this machine.
  """
  target_dir = source_dir if target_dir is None else target_dir
  source_dir = os.path.abspath(source_dir)
  target_dir = os.path.abspath(target_dir)
  source_basename = os.path.basename(source_dir)
  target_basename = os.path.basename(target_dir)
  if source_basename != target_basename:
    raise Exception("The source_dir and target_dir must have the same base "
                    "name, {} != {}".format(source_basename, target_basename))
  tarred_bytes = tarred_directory_as_bytes(source_dir)

  def f(worker_info):
    if worker_info["counter"] == 0:
      tarred_bytes_to_directory(tarred_bytes, os.path.dirname(target_dir))
    sys.path.append(os.path.dirname(target_dir))
  # Run this function on all workers to copy the directory to all nodes and to
  # add the directory to the Python path of each worker.
  ray.worker.global_worker.run_function_on_all_workers(f)
