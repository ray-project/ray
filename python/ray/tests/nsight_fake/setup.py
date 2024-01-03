from setuptools import setup

setup(
    name="nsys",
    version="0.0.1",
    entry_points={"console_scripts": ["nsys=nsys_fake:run"]},
)
