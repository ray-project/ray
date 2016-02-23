import sys

from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize

# because of relative paths, this must be run from inside orch/lib/orchpy/

MACOSX = (sys.platform in ['darwin'])

setup(
  name = "orchestra",
  version = "0.1.dev0",
  ext_modules = cythonize([
    Extension("orchpy/worker",
      include_dirs = ["../../src", "/usr/local/include/"],
      sources = ["orchpy/worker.pyx"],
      extra_link_args=["-Iorchpy -lorchlib"],
      language = "c++"),
    Extension("orchpy/unison",
      include_dirs = ["../../src/", "/usr/local/include/"],
      sources = ["orchpy/unison.pyx"],
      extra_link_args=["-Iorchpy -lorchlib"],
      language = "c++")],
    compiler_directives={'language_level': 2}), # switch to 3 for python 3
  use_2to3=True,
  packages=find_packages(),
  package_data = {
    'orchpy': ['liborchlib.dylib' if MACOSX else 'liborchlib.so',
               'scheduler_server',
               'objstore']
  },
  zip_safe=False
)
