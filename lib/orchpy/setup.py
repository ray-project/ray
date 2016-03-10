import sys

from setuptools import setup, Extension, find_packages
import setuptools
from Cython.Build import cythonize

# because of relative paths, this must be run from inside orch/lib/orchpy/

MACOSX = (sys.platform in ['darwin'])

setup(
  name = "orchestra",
  version = "0.1.dev0",
  use_2to3=True,
  packages=find_packages(),
  package_data = {
    'orchpy': ['liborchpylib.dylib' if MACOSX else 'liborchpylib.so',
               'scheduler',
               'objstore']
  },
  zip_safe=False
)
