from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re

from setuptools import setup


def find_version(*filepath):
    # Extract version information from filepath
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


setup(
    name="rllib",
    version=find_version("..", "python", "ray", "__init__.py"),
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description="A scalable and unified reinforcement learning library",
    url="https://github.com/ray-project/ray",
    packages=["rllib"],
    install_requires="ray[rllib]",
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
