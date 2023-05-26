(configure-logging)=

# Configuring Logging

This guide helps you understand and modify the configuration of Ray's logging system.

## Ray’s logging directory
By default, Ray log files are stored in a `/tmp/ray/session_*/logs` directory. View the [detailed logging structure](#) to understand how the log files are organized within the logs folder.

:::{note}
Ray uses ``/tmp/ray`` (for Linux and MacOS) as the default temp directory. To change the temp and the logging directory, specify it when you call ``ray start`` or ``ray.init()``.
:::

A new Ray session creates a new folder to the temp directory. The latest session folder is symlinked to `/tmp/ray/session_latest`. Here is an example temp directory:

```
├── tmp/ray
│   ├── session_latest
│   │   ├── logs
│   │   ├── ...
│   ├── session_2023-05-14_21-19-58_128000_45083
│   │   ├── logs
│   │   ├── ...
│   ├── session_2023-05-15_21-54-19_361265_24281
│   ├── ...
```

Usually, temp directories are cleared up whenever the machines reboot. As a result, log files may get lost whenever your cluster or some of the nodes are stopped or terminated.

If you need to inspect logs after the clusters are stopped or terminated, you need to store and persist the logs. View the instructions for how to process and export logs for [clusters on VMs](#) and [KubeRay Clusters](#).

## Ray's logging configuration
When ``import ray`` is executed, Ray's logger is initialized, generating a sensible configuration given in ``python/ray/_private/log.py``. The default logging level is ``logging.INFO``.

All Ray loggers are automatically configured in ``ray._private.ray_logging``. To modify the Ray logger:

```python
import logging

logger = logging.getLogger("ray")
logger # Modify the ray logging config
```
Similarly, to modify the logging configuration for Ray AIR or other libraries, specify the appropriate logger name:

```python
import logging

# First, get the handle for the logger you want to modify
ray_air_logger = logging.getLogger("ray.air")
ray_data_logger = logging.getLogger("ray.data")
ray_tune_logger = logging.getLogger("ray.tune")
ray_rllib_logger = logging.getLogger("ray.rllib")
ray_train_logger = logging.getLogger("ray.train")
ray_serve_logger = logging.getLogger("ray.serve")
ray_workflow_logger = logging.getLogger("ray.workflow")

# Modify the ray.data logging level
ray_data_logger.setLevel(logging.WARNING)

# Other loggers can be modified similarly.
# Here's how to add an aditional file handler for ray tune:
ray_tune_logger.addHandler(logging.FileHandler("extra_ray_tune_log.log"))
```

## Structured logging
Implementation of structured logging is usually recommended to make downstream users/applications consume the logs more efficiently.

### System logs
Ray’s system/component logs are mostly structured following this format. <br />
(placeholder for the format)

Example <br />
(placeholder for the example)

:::{note}
Some of the system component logs are not structured as suggested above as of 2.5. We are still in the process of migrating all the system logs to structured logs.
:::

### Application logs
A Ray applications include both driver and worker processes. For python applications, it’s recommended to use python loggers to format and structure your logs. 
As a result, python loggers need to be set up for both driver and worker processes.

::::{tab-set}

:::{tab-item} Ray Core
Set up the python logger for driver and worker processes separately:
1. Set up the logger for the driver process after importing `ray`.
2. Use `worker_process_setup_hook` to configure the python logger for all the worker processes.

![Set up python loggers](../images/setup-logger-application.png)

If you want to control the logger for particular actors or tasks, view 

:::

:::{tab-item} Ray AIR or other libraries
If you use Ray AIR or any of the Ray libraries, follow the instructions provided in the documentation for the library.
:::

::::

### Add metadata to structured logs
If you need additional metadata to make logs more structured, fetch the metadata of jobs, tasks or actors via Ray’s [`runtime_context APIs`](#). 
::::{tab-set}

:::{tab-item} Ray Job
...
:::

:::{tab-item} Ray Actor
...
:::

:::{tab-item} Ray Task
...
:::

:::{tab-item} AIR or library-specific entities
...
:::

::::

## Customizing worker process loggers

When using Ray, all tasks and actors are executed remotely in Ray's worker processes. To provide your own logging configuration for the worker processes, customize the worker loggers following the instructions below:
::::{tab-set}

:::{tab-item} Ray Core: individual worker process
Customize the logger configuration when you define the tasks or actors.
```python
import ray
import logging
# Initiate a driver.
ray.init()

@ray.remote
class Actor:
    def __init__(self):
        # Basic config automatically configures logs to
        # be streamed to stdout and stderr.
        # Set the severity to INFO so that info logs are printed to stdout.
        logging.basicConfig(level=logging.INFO)

    def log(self, msg):
        logger = logging.getLogger(__name__)
        logger.info(msg)

actor = Actor.remote()
ray.get(actor.log.remote("A log message for an actor."))

@ray.remote
def f(msg):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(msg)

ray.get(f.remote("A log message for a task."))
```

```bash
(Actor pid=179641) INFO:__main__:A log message for an actor.
(f pid=177572) INFO:__main__:A log message for a task.
```
:::

:::{tab-item} Ray Core: all worker processes of a job
Use `worker_process_setup_hook` to apply the new logging configuration to all worker processes within a job.
```python
# driver.py
def logging_setup_func():
    logger = logging.getLogger("ray")
    logger.setLevel(logging.DEBUG)
    warnings.simplefilter("always")

ray.init(runtime_env={"worker_process_setup_hook": logging_setup_func})

logging_setup_func()
```
:::

:::{tab-item} Ray AIR or other libraries
If you use Ray AIR or any of the Ray libraries, follow the instructions provided in the documentation for the library.
:::

::::



## Logging to the driver
By default, all stdout and stderr of tasks and actors are streamed to the Ray driver (the entrypoint script that calls ``ray.init``). It helps users aggregate the logs for the distributed Ray application in a single place.

```{literalinclude} doc_code/app_logging.py
```

All stdout emitted from the ``print`` method is printed to the driver with a ``(the task or actor repr, the process ID, IP address)`` prefix.

``` bash
(pid=45601) task
(Actor pid=480956) actor
```

### Customizing prefixes for actor logs

It is often useful to distinguish between log messages from different actors. For example, suppose you have a large number of worker actors. In this case, you may want to be able to easily see the index of the actor that logged a particular message. This can be achieved by defining the `__repr__ <https://docs.python.org/3/library/functions.html#repr>`__ method for an actor class. When defined, the actor repr will be used in place of the actor name. For example:

```{literalinclude} /ray-core/doc_code/actor-repr.py
```

This produces the following output:

```bash
(MyActor(index=2) pid=482120) hello there
(MyActor(index=1) pid=482119) hello there
```

### Coloring Actor log prefixes
By default Ray prints Actor logs prefixes in light blue:
Users may instead activate multi-color prefixes by setting the environment variable ``RAY_COLOR_PREFIX=1``.
This will index into an array of colors modulo the PID of each process.

![coloring-actor-log-prefixes](../images/coloring-actor-log-prefixes.png){align=center}

### Disable logging to the driver
In large scale runs, it may be undesirable to route all worker logs to the driver. You can disable this feature by setting ``log_to_driver=False`` in Ray init:

```python
import ray

# Task and actor logs will not be copied to the driver stdout.
ray.init(log_to_driver=False)
```


## Log deduplication

By default, Ray deduplicates logs that appear redundantly across multiple processes. The first instance of each log message is always immediately printed. However, subsequent log messages of the same pattern (ignoring words with numeric components) are buffered for up to five seconds and printed in batch. For example, for the following code snippet:

```python
import ray
import random

@ray.remote
def task():
    print("Hello there, I am a task", random.random())

ray.get([task.remote() for _ in range(100)])
```

The output is as follows:

```bash
2023-03-27 15:08:34,195	INFO worker.py:1603 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265 
(task pid=534172) Hello there, I am a task 0.20583517821231412
(task pid=534174) Hello there, I am a task 0.17536720316370757 [repeated 99x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication)
```

This feature is especially useful when importing libraries such as `tensorflow` or `numpy`, which may emit many verbose warning messages when imported. You can configure this feature as follows:

1. Set ``RAY_DEDUP_LOGS=0`` to disable this feature entirely.
2. Set ``RAY_DEDUP_LOGS_AGG_WINDOW_S=<int>`` to change the agggregation window.
3. Set ``RAY_DEDUP_LOGS_ALLOW_REGEX=<string>`` to specify log messages to never deduplicate.
4. Set ``RAY_DEDUP_LOGS_SKIP_REGEX=<string>`` to specify log messages to skip printing.



## Distributed progress bars (tqdm)

When using `tqdm <https://tqdm.github.io>`__ in Ray remote tasks or actors, you may notice that the progress bar output is corrupted. To avoid this problem, you can use the Ray distributed tqdm implementation at ``ray.experimental.tqdm_ray``:

```{literalinclude} /ray-core/doc_code/tqdm.py
```

This tqdm implementation works as follows:

1. The ``tqdm_ray`` module translates TQDM calls into special json log messages written to worker stdout.
2. The Ray log monitor, instead of copying these log messages directly to the driver stdout, routes these messages to a tqdm singleton.
3. The tqdm singleton determines the positions of progress bars from various Ray tasks / actors, ensuring they don't collide or conflict with each other.

Limitations:

- Only a subset of tqdm functionality is supported. Refer to the ray_tqdm `implementation <https://github.com/ray-project/ray/blob/master/python/ray/experimental/tqdm_ray.py>`__ for more details.
- Performance may be poor if there are more than a couple thousand updates per second (updates are not batched).

By default, the builtin print will also be patched to use `ray.experimental.tqdm_ray.safe_print` when `tqdm_ray` is used.
This avoids progress bar corruption on driver print statements. To disable this, set `RAY_TQDM_PATCH_PRINT=0`.


