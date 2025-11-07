"""
Apache Iceberg datasource and datasink for Ray Data.

This package provides support for reading from and writing to Apache Iceberg tables
using PyIceberg. It includes:

- IcebergDatasource: Read Iceberg tables into Ray Datasets
- IcebergDatasink: Write Ray Datasets to Iceberg tables

For more information on Apache Iceberg, see: https://iceberg.apache.org/
For more information on PyIceberg, see: https://py.iceberg.apache.org/
"""

from ray.data._internal.datasource.iceberg.datasink import IcebergDatasink
from ray.data._internal.datasource.iceberg.datasource import IcebergDatasource

__all__ = [
    "IcebergDatasource",
    "IcebergDatasink",
]
