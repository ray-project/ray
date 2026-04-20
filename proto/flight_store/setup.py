import numpy as np
import pyarrow as pa
from Cython.Build import cythonize
from setuptools import Extension, setup

# PyArrow ships Arrow C++ headers and libraries.
pa_include = pa.get_include()
pa_lib_dirs = pa.get_library_dirs()

# Determine Arrow Flight library name (varies by platform/version).
# Try arrow_flight first, fall back to arrow_flight_sql, etc.
arrow_libs = ["arrow", "arrow_flight", "arrow_python"]

ext = Extension(
    "arrow_flight_store",
    sources=["arrow_flight_store.pyx", "arrow_flight_store.cc"],
    include_dirs=[pa_include, np.get_include(), "."],
    library_dirs=pa_lib_dirs,
    libraries=arrow_libs,
    language="c++",
    extra_compile_args=["-std=c++17", "-O2", "-DARROW_NO_DEPRECATED_API"],
    extra_link_args=[f"-Wl,-rpath,{pa_lib_dirs[0]}"] if pa_lib_dirs else [],
)

setup(
    name="arrow_flight_store",
    ext_modules=cythonize(
        [ext],
        compiler_directives={"language_level": "3"},
    ),
)
