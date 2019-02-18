from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


@ray.remote
def echo(x):
    return x


@ray.remote
class WithConstructor(object):
    def __init__(self, data):
        self.init_data = data
        self.data = None

    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data

    def get_init_data(self):
        return self.init_data


@ray.remote
class WithoutConstructor(object):
    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data


class BaseClass(object):
    def __init__(self, data):
        self.data = None

    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data


@ray.remote
class DerivedClass(BaseClass):
    def __init__(self, data):
        # Due to different behaviors of super in Python 2 and Python 3,
        # we use BaseClass directly here.
        BaseClass.__init__(self, data)
        self.new_data = "DerivedClass" + data

    def get_new_data(self):
        return self.new_data
