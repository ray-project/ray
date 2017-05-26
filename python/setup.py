from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess

from setuptools import setup, find_packages, Extension, Distribution
import setuptools.command.build_ext as _build_ext


class build_ext(_build_ext.build_ext):
  def run(self):
    subprocess.check_call(["../build.sh"])


package_data = {
    "ray": ["core/src/common/thirdparty/redis/src/redis-server",
            "core/src/common/redis_module/libray_redis_module.so",
            "core/src/plasma/plasma_store",
            "core/src/plasma/plasma_manager",
            "core/src/plasma/libplasma.so",
            "core/src/local_scheduler/local_scheduler",
            "core/src/local_scheduler/liblocal_scheduler_library.so",
            "core/src/numbuf/libarrow.so",
            "core/src/numbuf/libnumbuf.so",
            "core/src/global_scheduler/global_scheduler"]
}


class BinaryDistribution(Distribution):
  def has_ext_modules(foo):
    return True


setup(name="ray",
      version="0.1.0",
      packages=find_packages(),
      package_data=package_data,
      cmdclass={"build_ext": build_ext},
      distclass=BinaryDistribution,
      # Dummy extension to trigger build_ext
      #ext_modules=[Extension('__dummy__', sources=[])],
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
