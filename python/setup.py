from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess

from setuptools import setup, find_packages, Extension, Distribution
import setuptools.command.build_ext as _build_ext


class build_ext(_build_ext.build_ext):
  def run(self):
    subprocess.check_call(["../build.sh"])


class BinaryDistribution(Distribution):
  def has_ext_modules(self):
    return True


setup(name="ray",
      version="0.1.0",
      packages=find_packages(),
      # Dummy extension to trigger build_ext
      ext_modules=[Extension("__dummy__", sources=[])],
      cmdclass={"build_ext": build_ext},
      distclass=BinaryDistribution,
      install_requires=["numpy",
                        "funcsigs",
                        "colorama",
                        "psutil",
                        "redis",
                        "cloudpickle >= 0.2.2",
                        "flatbuffers"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")
