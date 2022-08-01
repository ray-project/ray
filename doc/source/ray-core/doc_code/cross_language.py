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

with ray.init(job_config=ray.job_config.JobConfig(code_search_path=["/home/ray/java_build/"])):
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