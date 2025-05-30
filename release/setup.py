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
        "aioboto3",
        "anyscale >= 0.26.1",
        "aws_requests_auth",
        "bazel-runfiles",
        "boto3",
        "click",
        "freezegun",
        "google-cloud-storage",
        "jinja2",
        "protobuf >= 3.15.3, != 3.19.5",
        "pytest",
        "pyyaml",
        "pybuildkite",
        "pydantic >= 2.5.0",
        "PyGithub",
        "requests",
        "twine == 6.1.0",
        "docker >= 7.1.0",
    ],
)
