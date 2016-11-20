import subprocess
from setuptools import setup, find_packages
import setuptools.command.install as _install
from sys import platform

extension = ""
if platform == "linux" or platform == "linux2":
  extension = ".so"
elif platform == "darwin":
  extension = ".dylib"

# Because of relative paths, this must be run from inside numbuf/.

class install(_install.install):
  def run(self):
    subprocess.check_call(["./setup.sh"])
    subprocess.check_call(["./build.sh"])
    # Calling _install.install.run(self) does not fetch required packages and
    # instead performs an old-style install. See command/install.py in
    # setuptools. So, calling do_egg_install() manually here.
    self.do_egg_install()

setup(name="numbuf",
      version="0.0.1",
      packages=find_packages(),
      package_data={"numbuf": ["libnumbuf.so",
                               "libarrow" + extension,
                               "libarrow_io" + extension,
                               "libarrow_ipc" + extension]},
      cmdclass={"install": install},
      setup_requires=["numpy"],
      include_package_data=True,
      zip_safe=False)
