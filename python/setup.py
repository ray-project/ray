from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
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

setup(name="ray",
      version="0.0.1",
      packages=find_packages(),
      package_data={"common": ["thirdparty/redis/src/redis-server",
                               "redis_module/ray_redis_module.so"],
                    "plasma": ["plasma_store",
                               "plasma_manager",
                               "libplasma.so"],
                    "photon": ["build/photon_scheduler",
                               "photon/libphoton.so"],
                    "numbuf": ["libarrow.so",
                               "libnumbuf.so"],
                    "global_scheduler": ["build/global_scheduler"]},
      cmdclass={"install": install},
      install_requires=["numpy",
                        "funcsigs",
                        "colorama",
                        "psutil",
                        "redis",
                        "cloudpickle >= 0.2.2"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")
