# flake8: noqa

# fmt: off
# __crosslang_init_start__
import ray

ray.init(job_config=ray.job_config.JobConfig(code_search_path=["/path/to/code"]))
# __crosslang_init_end__
# fmt: on

# fmt: off
# __crosslang_multidir_start__
import ray

ray.init(job_config=ray.job_config.JobConfig(code_search_path="/path/to/jars:/path/to/pys"))
# __crosslang_multidir_end__
# fmt: on

# fmt: off
# __python_call_java_start__
import ray

with ray.init(job_config=ray.job_config.JobConfig(code_search_path=["/path/to/code"])):
  # Define a Java class.
  counter_class = ray.cross_language.java_actor_class(
        "io.ray.demo.Counter")

  # Create a Java actor and call actor method.
  counter = counter_class.remote()
  obj_ref1 = counter.increment.remote()
  assert ray.get(obj_ref1) == 1
  obj_ref2 = counter.increment.remote()
  assert ray.get(obj_ref2) == 2

  # Define a Java function.
  add_function = ray.cross_language.java_function(
        "io.ray.demo.Math", "add")

  # Call the Java remote function.
  obj_ref3 = add_function.remote(1, 2)
  assert ray.get(obj_ref3) == 3
# __python_call_java_end__
# fmt: on

# fmt: off
# __python_module_start__
# ray_demo.py

import ray

@ray.remote
class Counter(object):
  def __init__(self):
      self.value = 0

  def increment(self):
      self.value += 1
      return self.value

@ray.remote
def add(a, b):
    return a + b
# __python_module_end__
# fmt: on

# fmt: off
# __serialization_start__
# ray_serialization.py

import ray

@ray.remote
def py_return_input(v):
    return v
# __serialization_end__
# fmt: on

# fmt: off
# __raise_exception_start__
# ray_exception.py

import ray

@ray.remote
def raise_exception():
    1 / 0
# __raise_exception_end__
# fmt: on

# fmt: off
# __raise_exception_demo_start__
# ray_exception_demo.py

import ray

with ray.init(job_config=ray.job_config.JobConfig(code_search_path=["/path/to/ray_exception"])):
  obj_ref = ray.cross_language.java_function(
        "io.ray.demo.MyRayClass",
        "raiseExceptionFromPython").remote()
  ray.get(obj_ref)  # <-- raise exception from here.
# __raise_exception_demo_end__
# fmt: on
