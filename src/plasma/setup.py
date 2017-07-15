from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from setuptools import setup, find_packages
import setuptools.command.install as _install

import subprocess


class install(_install.install):
    def run(self):
        subprocess.check_call(["make"])
        subprocess.check_call(["cp", "build/plasma_store",
                               "plasma/plasma_store"])
        subprocess.check_call(["cp", "build/plasma_manager",
                               "plasma/plasma_manager"])
        subprocess.check_call(["cmake", ".."], cwd="./build")
        subprocess.check_call(["make", "install"], cwd="./build")
        # Calling _install.install.run(self) does not fetch required packages
        # and instead performs an old-style install. See command/install.py in
        # setuptools. So, calling do_egg_install() manually here.
        self.do_egg_install()


setup(name="Plasma",
      version="0.0.1",
      description="Plasma client for Python",
      packages=find_packages(),
      package_data={"plasma": ["plasma_store",
                               "plasma_manager",
                               "libplasma.so"]},
      cmdclass={"install": install},
      include_package_data=True,
      zip_safe=False)
