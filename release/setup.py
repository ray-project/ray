from setuptools import find_packages, setup

setup(
    name="ray_release",
    packages=find_packages(
        where=".",
        include=[
            package for package in find_packages() if package.startswith("ray_release")
        ],
    ),
    version="0.0.1",
    author="Ray Team",
    description="The Ray OSS release testing package",
    url="https://github.com/ray-project/ray",
    install_requires=[
        # Keep this in sync with requirements_buildkite.in
        "anyscale",
        "bazel-runfiles",
        "boto3",
        "click",
        "freezegun",
        "google-cloud-storage",
        "jinja2",
        "protobuf",
        "pydantic >= 2.5.0",
        "pytest",
        "pyyaml",
        "pybuildkite",
        "PyGithub",
        "requests",
        "retry",
    ],
)
