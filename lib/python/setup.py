import sys

from setuptools import setup, Extension, find_packages
import setuptools

# because of relative paths, this must be run from inside ray/lib/python/

setup(
  name = "ray",
  version = "0.1.dev0",
  use_2to3=True,
  packages=find_packages(),
  package_data = {
    "ray": ["libraylib.so", "scheduler", "objstore"]
  },
  zip_safe=False
)
