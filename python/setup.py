from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess

from setuptools import setup, find_packages
import setuptools.command.install as _install


class install(_install.install):
  def run(self):
    subprocess.check_call(["../build.sh"])
    # Calling _install.install.run(self) does not fetch required packages and
    # instead performs an old-style install. See command/install.py in
    # setuptools. So, calling do_egg_install() manually here.
    self.do_egg_install()


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

setup(name="ray",
      version="0.1.0",
      packages=find_packages(),
      package_data=package_data,
      cmdclass={"install": install},
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
