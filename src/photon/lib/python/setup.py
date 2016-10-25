from setuptools import setup, find_packages, Extension

photon_module = Extension("photon",
                          sources=["photon_extension.c", "../../common/lib/python/common_extension.c"],
                          include_dirs=["../../", "../../common/",
                                        "../../common/thirdparty/",
                                        "../../common/lib/python"],
                          extra_objects=["../../build/photon_client.a", "../../common/build/libcommon.a"],
                          extra_compile_args=["--std=c99", "-Werror"])

setup(name="Photon",
      version="0.1",
      description="Photon library for Ray",
      ext_modules=[photon_module])
