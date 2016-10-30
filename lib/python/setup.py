import subprocess

from setuptools import setup, find_packages
import setuptools.command.install as _install

# Because of relative paths, this must be run from inside ray/lib/python/.

class install(_install.install):
  def run(self):
    subprocess.check_call(["../../build.sh"])
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
                               "build/plasma_client.so"],
                    "photon": ["build/photon_scheduler",
                               "lib/python/photon.so"]},
      cmdclass={"install": install},
      include_package_data=True,
      zip_safe=False)
