from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray


def setup():
    if not hasattr(setup, "is_initialized"):
        ray.init(num_cpus=10, resources={"foo": 1})
        setup.is_initialized = True


def square(x):
    return x * x


class TaskSuite(object):
    timeout = 10

    def setup(self):
        self.square = ray.remote(square)

    def run_many_tasks(self):
        ray.get([self.square.remote(i) for i in range(100)])

    def run_task_dependency(self):
        first_oid = self.square.remote(2)
        second_oid = self.square.remote(first_oid)
        ray.get(second_oid)

    def time_submit_task(self):
        self.square.remote(2)

    def time_task_lifecycle(self):
        ray.get(self.square.remote(2))

    def peakmem_task_lifecycle(self):
        ray.get(self.square.remote(2))

    def time_run_many_tasks(self):
        self.run_many_tasks()

    def peakmem_run_many_tasks(self):
        self.run_many_tasks()

    def time_task_dependency(self):
        self.run_task_dependency()

    def peakmem_task_dependency(self):
        self.run_task_dependency()


class CPUTaskSuite(TaskSuite):
    def setup(self):
        self.square = ray.remote(num_cpus=1)(square)


class CustomResourceTaskSuite(TaskSuite):
    def setup(self):
        self.square = ray.remote(resources={"foo": 1})(square)
