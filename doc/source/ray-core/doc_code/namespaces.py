# flake8: noqa
# fmt: off
# __init_namespace_start__
import ray

ray.init(namespace="hello")
# __init_namespace_end__
# fmt: on

ray.shutdown()

# fmt: off
# __actor_namespace_start__
import subprocess
import ray

try:
    subprocess.check_output(["ray", "start", "--head"])

    @ray.remote
    class Actor:
      pass

    # Job 1 creates two actors, "orange" and "purple" in the "colors" namespace.
    with ray.init("ray://localhost:10001", namespace="colors"):
      Actor.options(name="orange", lifetime="detached").remote()
      Actor.options(name="purple", lifetime="detached").remote()

    # Job 2 is now connecting to a different namespace.
    with ray.init("ray://localhost:10001", namespace="fruits"):
      # This fails because "orange" was defined in the "colors" namespace.
      try:
        ray.get_actor("orange")
      except ValueError:
        pass

      # This succceeds because the name "orange" is unused in this namespace.
      Actor.options(name="orange", lifetime="detached").remote()
      Actor.options(name="watermelon", lifetime="detached").remote()

    # Job 3 connects to the original "colors" namespace
    context = ray.init("ray://localhost:10001", namespace="colors")

    # This fails because "watermelon" was in the fruits namespace.
    try:
      ray.get_actor("watermelon")
    except ValueError:
      pass

    # This returns the "orange" actor we created in the first job, not the second.
    ray.get_actor("orange")

    # We are manually managing the scope of the connection in this example.
    context.disconnect()
finally:
    subprocess.check_output(["ray", "stop", "--force"])
# __actor_namespace_end__
# fmt: on

# fmt: off
# __specify_actor_namespace_start__
import subprocess
import ray

try:
    subprocess.check_output(["ray", "start", "--head"])

    @ray.remote
    class Actor:
        pass

    ctx = ray.init("ray://localhost:10001")

    # Create an actor with specified namespace.
    Actor.options(name="my_actor", namespace="actor_namespace", lifetime="detached").remote()

    # It is accessible in its namespace.
    ray.get_actor("my_actor", namespace="actor_namespace")
    ctx.disconnect()
finally:
    subprocess.check_output(["ray", "stop", "--force"])
# __specify_actor_namespace_end__
# fmt: on

# fmt: off
# __anonymous_namespace_start__
import subprocess
import ray

try:
  subprocess.check_output(["ray", "start", "--head"])

  @ray.remote
  class Actor:
      pass

  # Job 1 connects to an anonymous namespace by default
  with ray.init("ray://localhost:10001"):
    Actor.options(name="my_actor", lifetime="detached").remote()

  # Job 2 connects to a _different_ anonymous namespace by default
  with ray.init("ray://localhost:10001"):
    # This succeeds because the second job is in its own namespace.
    Actor.options(name="my_actor", lifetime="detached").remote()

finally:
    subprocess.check_output(["ray", "stop", "--force"])
# __anonymous_namespace_end__
# fmt: on

# fmt: off
# __get_namespace_start__
import subprocess
import ray

try:
  subprocess.check_output(["ray", "start", "--head"])

  ray.init(address="auto", namespace="colors")

  # Will print namespace name "colors".
  print(ray.get_runtime_context().namespace)

finally:
    subprocess.check_output(["ray", "stop", "--force"])
# __get_namespace_end__
# fmt: on
