from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=0)
        setup.is_initialized = True


def square(x):
    return x * x


class Foo(object):
    def bar(self):
        return 42


class PutBase(object):
    def setup(self):
        self.object = None

    def time_put(self):
        ray.put(self.object)

    def peakmem_put(self):
        ray.put(self.object)


class PutBoolSuite(PutBase):
    def setup(self):
        self.object = True


class PutIntSuite(PutBase):
    def setup(self):
        self.object = 42


class PutFloatSuite(PutBase):
    def setup(self):
        self.object = 4.2


class PutComplexSuite(PutBase):
    def setup(self):
        self.object = 4 + 2j


class PutNoneSuite(PutBase):
    def setup(self):
        self.object = None


class PutStringSuite(PutBase):
    def setup(self):
        self.object = "forty-two"


class PutBytesSuite(PutBase):
    def setup(self):
        self.object = b"forty-two"


class PutListSuite(PutBase):
    def setup(self):
        self.object = [i for i in range(100)]


class PutSetSuite(PutBase):
    def setup(self):
        self.object = {i for i in range(100)}


class PutTupleSuite(PutBase):
    def setup(self):
        self.object = tuple(range(100))


class PutDictSuite(PutBase):
    def setup(self):
        self.object = {i: i for i in range(100)}


class PutFunctionSuite(PutBase):
    def setup(self):
        self.object = square


class PutClassSuite(PutBase):
    def setup(self):
        self.object = Foo


class PutClassInstanceSuite(PutBase):
    def setup(self):
        self.object = Foo()


class PutArraySuite(PutBase):
    def setup(self):
        self.object = np.random.random((100, 100, 100))
