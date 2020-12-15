import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="mlflow_ray_serve",
    version="0.0.1",
    # author="hhsecond",
    # author_email="sherin@tensorwerk.com",
    # description="MLFlow + Ray Serve integration package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # url="https://github.com/RedisAI/mlflow-redisai",
    packages=setuptools.find_packages(),
    # classifiers=[
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: Apache Software License",
    #     "Operating System :: OS Independent",
    # ],
    python_requires='>=3.6',
    install_requires=["ray[serve]", "mlflow"],
    entry_points={"mlflow.deployments": "ray-serve=mlflow_ray_serve"}
)