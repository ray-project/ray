import sys

from setuptools import setup, Extension, find_packages
import setuptools

# because of relative paths, this must be run from inside halo/lib/python/

MACOSX = (sys.platform in ["darwin"])

setup(
  name = "halo",
  version = "0.1.dev0",
  use_2to3=True,
  packages=find_packages(),
  package_data = {
    "halo": ["libhalolib.dylib" if MACOSX else "libhalolib.so",
               "scheduler",
               "objstore"]
  },
  zip_safe=False
)
