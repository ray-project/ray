#!/usr/bin/env python
from setuptools import setup, Extension

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='cloudpickle-generators',
    version='0.1.0',
    description=('cloudpickle support for generators,'
                 ' including partially consumed generators.'),
    author='Joe Jevnik',
    author_email='joejev@gmail.com',
    packages=['cloudpickle_generators'],
    include_package_data=True,
    long_description=long_description,
    license='BSD-3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Topic :: Software Development',
        'Topic :: Utilities',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
    ],
    url='https://github.com/llllllllll/cloudpickle-generators',
    ext_modules=[
        Extension('cloudpickle_generators._core',
                  ['cloudpickle_generators/_core.c'],
                  extra_compile_args=[
                      '-Wall',
                      '-Wextra',
                      '-Wno-missing-field-initializers',
                  ]),
    ],
)
