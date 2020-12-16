import setuptools

setuptools.setup(
    name="mlflow_ray_serve",
    version="0.0.1",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    install_requires=["ray[serve]", "mlflow"],
    entry_points={"mlflow.deployments": "ray-serve=mlflow_ray_serve"})
