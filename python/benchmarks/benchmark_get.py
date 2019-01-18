from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=4)
        setup.is_initialized = True


def square(x):
    return x * x


class Foo(object):
    def bar(self):
        return 42


class GetBase(object):
    def setup(self):
        self.oid = ray.put(None)

    def time_get(self):
        ray.get(self.oid)

    def peakmem_get(self):
        ray.get(self.oid)


class GetBoolSuite(GetBase):
    def setup(self):
        self.oid = ray.put(True)


class GetIntSuite(GetBase):
    def setup(self):
        self.oid = ray.put(42)


class GetFloatSuite(GetBase):
    def setup(self):
        self.oid = ray.put(4.2)


class GetComplexSuite(GetBase):
    def setup(self):
        self.oid = ray.put(4 + 2j)


class GetNoneSuite(GetBase):
    def setup(self):
        self.oid = ray.put(None)


class GetStringSuite(GetBase):
    def setup(self):
        self.oid = ray.put("forty-two")


class GetBytesSuite(GetBase):
    def setup(self):
        self.oid = ray.put(b"forty-two")


class GetListSuite(GetBase):
    def setup(self):
        self.oid = ray.put([i for i in range(100)])


class GetSetSuite(GetBase):
    def setup(self):
        self.oid = ray.put({i for i in range(100)})


class GetTupleSuite(GetBase):
    def setup(self):
        self.oid = ray.put(tuple(range(100)))


class GetDictSuite(GetBase):
    def setup(self):
        self.oid = ray.put({i: i for i in range(100)})


class GetFunctionSuite(GetBase):
    def setup(self):
        self.oid = ray.put(square)


class GetClassSuite(GetBase):
    def setup(self):
        self.oid = ray.put(Foo)


class GetClassInstanceSuite(GetBase):
    def setup(self):
        self.oid = ray.put(Foo())


class GetArraySuite(GetBase):
    def setup(self):
        self.oid = ray.put(np.random.random((100, 100, 100)))
