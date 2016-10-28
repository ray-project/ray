import subprocess
from setuptools import setup, find_packages, Extension
import setuptools.command.install as _install

# Because of relative paths, this must be run from inside numbuf/.

class install(_install.install):
  def run(self):
    subprocess.check_call(["./setup.sh"])
    subprocess.check_call(["./build.sh"])
    subprocess.check_call(["cp", "libnumbuf.so", "numbuf/"])
    # Calling _install.install.run(self) does not fetch required packages and
    # instead performs an old-style install. See command/install.py in
    # setuptools. So, calling do_egg_install() manually here.
    self.do_egg_install()

setup(name="numbuf",
      version="0.0.1",
      packages=find_packages(),
      package_data={"numbuf": ["libnumbuf.so"]},
      cmdclass={"install": install},
      include_package_data=True,
      zip_safe=False)
