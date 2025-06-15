from setuptools import setup, find_packages

setup(
    name="depsets",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "depsets=dependencies.cli:cli",
        ],
    },
    python_requires=">=3.7",
    include_package_data=True,
    zip_safe=False,
)