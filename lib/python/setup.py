from setuptools import setup, find_packages, Extension

common_module = Extension("common",
                          sources=["common_module.c", "common_extension.c"],
                          include_dirs=["../../", "../../thirdparty"],
                          extra_objects=["../../build/libcommon.a"],
                          extra_compile_args=["--std=c99", "-Werror"])

setup(name="Common",
      version="0.1",
      description="Common library for Ray",
      ext_modules=[common_module])
