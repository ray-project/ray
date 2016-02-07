from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize

# because of relative paths, this must be run from inside orch/lib/orchpy/

setup(
  name = "orchestra",
  version = "0.1.dev0",
  ext_modules = cythonize([
    Extension("orchpy/context",
      sources = ["orchpy/context.pyx"], libraries=["orchlib"],
      library_dirs=['../orchlib/'])],
    compiler_directives={'language_level': 3}),
  use_2to3=True,
  packages=find_packages()
)
