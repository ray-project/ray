import os
import subprocess

from setuptools import setup, find_packages
import setuptools.command.install as _install

datadir = "./webui"
datafiles = [(root, [os.path.join(root, f) for f in files])
    for root, dirs, files in os.walk(datadir)]

class install(_install.install):
  def run(self):
    subprocess.check_call(["../../build.sh"])
    subprocess.check_call(["../../build-webui.sh"])
    # Calling _install.install.run(self) does not fetch required packages and
    # instead performs an old-style install. See command/install.py in
    # setuptools. So, calling do_egg_install() manually here.
    self.do_egg_install()

setup(name="ray",
      version="0.0.1",
      packages=find_packages(),
      package_data={"common": ["thirdparty/redis-3.2.3/src/redis-server"],
                    "plasma": ["build/plasma_store",
                               "build/plasma_manager",
                               "lib/python/libplasma.so"],
                    "photon": ["build/photon_scheduler",
                               "libphoton.so"]},
      data_files=datafiles,
      cmdclass={"install": install},
      install_requires=["numpy",
                        "funcsigs",
                        "colorama",
                        "psutil",
                        "redis",
                        "cloudpickle",
                        "numbuf==0.0.1"],
      include_package_data=True,
      zip_safe=False,
      license="Apache 2.0")
