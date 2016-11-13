from setuptools import setup, find_packages
import setuptools.command.install as _install

import subprocess

class install(_install.install):
  def run(self):
    subprocess.check_call(["make"], cwd="../../")
    subprocess.check_call(["cmake", ".."], cwd="../../build")
    subprocess.check_call(["make", "install"], cwd="../../build")
    # Calling _install.install.run(self) does not fetch required packages and
    # instead performs an old-style install. See command/install.py in
    # setuptools. So, calling do_egg_install() manually here.
    self.do_egg_install()

setup(name="Plasma",
      version="0.0.1",
      description="Plasma client for Python",
      packages=find_packages(),
      package_data={"plasma": ["libplasma.so"]},
      cmdclass={"install": install},
      include_package_data=True,
      zip_safe=False)
