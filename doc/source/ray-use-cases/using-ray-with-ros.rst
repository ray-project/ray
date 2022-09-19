Using Ray with the Robotics Operating System
============================================

The `Robotics Operating System (ROS) <https://www.ros.org/>`__ is a set of software libraries and tools
that help you build robot applications. You can use Ray to run your ROS application in a distributed way,
for example by:

- parallelizing simulations,
- data processing for preparing training data using ROS libraries,
- running reinforcement learning with a ROS environment

Setting up the environment
--------------------------

The ROS distribution consists of a collection of libraries and we need to make sure they can be found
by Ray's worker processes. ROS typically uses a bash script `/path/to/ros/setup.bash` to
set up the required environment variables and you can propagate this information to Ray using
:ref:`runtime environments <runtime-environments>` like this

.. code-block:: python

    import subprocess
    import ray

    # Set up ROS environment for the Ray driver process
    output = subprocess.check_output("source /path/to/ros/setup.bash; env -0", shell=True, executable="/bin/bash")

    # Parse the environment created by `setup.bash`
    newenv = dict(line.partition("=")[::2] for line in output.decode().split("\0"))

    # Set up a runtime environment with that environment so all Ray worker processes have access to it
    ray.init(runtime_env={"env_vars": newenv})


Communicating between Ray and ROS
---------------------------------

There is no one size fits all solution for this. Some ROS libraries already have Python wrappers, in that case
it will be easiest to call those libraries from Ray through the Python wrappers. If no wrappers are
available, you can start the ROS application through the Python `subprocess` package and communicate with the
application through pipes, sockets or files. You can also
`write your own Python wrappers <http://wiki.ros.org/ROS/Tutorials/Using%20a%20C%2B%2B%20class%20in%20Python>`__.
If you want to interact with your ROS solution via the OpenAI Gym API, check out the
`ROS OpenAI Gym wiki page <http://wiki.ros.org/openai_ros>`__.


Contribute to this document!
----------------------------

If you are using Ray to distribute your ROS application and have tips to share, create a PR and add to this
document!