(configure-logging)=

# Configuring Logging

This guide helps you understand and modify the configuration of Ray's logging system.

(logging-directory)=
## Logging directory
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

(logging-directory-structure)=
## Log files in logging directory

Here lists the log files in the logging directory. Broadly speaking, there are two types of log files: system log files and application log files.
Note that ``.out`` is logs from stdout/stderr and ``.err`` is logs from stderr. The backward compatibility of log directories is not guaranteed.

:::{note}
System logs may include info about your applications. For example, ``runtime_env_setup-[job_id].log`` may include info about you application's environment and depdenency.
:::

### Application logs
- ``job-driver-[submission_id].log``: The stdout of a job submitted via the :ref:`Ray Jobs API <jobs-overview>`.
- ``worker-[worker_id]-[job_id]-[pid].[out|err]``: Python or Java part of Ray drivers and workers. All of stdout and stderr from tasks or actors are streamed here. Note that job_id is an id of the driver.

### System (component) logs
- ``dashboard.[log|err]``: A log file of a Ray dashboard. ``log.`` file contains logs generated from the dashboard's logger. ``.err`` file contains stdout and stderr printed from the dashboard. They are usually empty except when the dashboard crashes unexpectedly.
- ``dashboard_agent.log``: Every Ray node has one dashboard agent. This is a log file of the agent.
- ``gcs_server.[out|err]``: The GCS server is a stateless server that manages Ray cluster metadata. It exists only in the head node.
- ``io-worker-[worker_id]-[pid].[out|err]``: Ray creates IO workers to spill/restore objects to external storage by default from Ray 1.3+. This is a log file of IO workers.
- ``log_monitor.[log|err]``: The log monitor is in charge of streaming logs to the driver. ``log.`` file contains logs generated from the log monitor's logger. ``.err`` file contains the stdout and stderr printed from the log monitor. They are usually empty except when the log monitor crashes unexpectedly.
- ``monitor.[out|err]``: Stdout and stderr of a cluster launcher.
- ``monitor.log``: Ray's cluster launcher is operated with a monitor process. It also manages the autoscaler.
- ``plasma_store.[out|err]``: Deprecated.
- ``python-core-driver-[worker_id]_[pid].log``: Ray drivers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``python-core-worker-[worker_id]_[pid].log``: Ray workers consist of CPP core and Python/Java frontend. This is a log file generated from CPP code.
- ``raylet.[out|err]``: A log file of raylets.
- ``redis-shard_[shard_index].[out|err]``: Redis shard log files.
- ``redis.[out|err]``: Redis log files.
- ``runtime_env_agent.log``: Every Ray node has one agent that manages :ref:`runtime environment <runtime-environments>` creation, deletion and caching.
  This is the log file of the agent containing logs of create/delete requests and cache hits and misses.
  For the logs of the actual installations (including e.g. ``pip install`` logs), see the ``runtime_env_setup-[job_id].log`` file (see below).
- ``runtime_env_setup-ray_client_server_[port].log``: Logs from installing {ref}`runtime environments <runtime-environments>` for a job when connecting via {ref}`Ray Client <ray-client-ref>`.
- ``runtime_env_setup-[job_id].log``: Logs from installing {ref}`runtime environments <runtime-environments>` for a task, actor or job.  This file will only be present if a runtime environment is installed.


## Log rotation

Ray supports log rotation of log files. Note that not all components are currently supporting log rotation. (Raylet and Python/Java worker logs are not rotating).

By default, logs are rotating when it reaches to 512MB (maxBytes), and there could be up to 5 backup files (backupCount). Indexes are appended to all backup files (e.g., `raylet.out.1`)
If you'd like to change the log rotation configuration, you can do it by specifying environment variables. For example,

```bash
RAY_ROTATION_MAX_BYTES=1024; ray start --head # Start a ray instance with maxBytes 1KB.
RAY_ROTATION_BACKUP_COUNT=1; ray start --head # Start a ray instance with backupCount 1.
```

## Using Ray's logger
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
(structured-logging)=
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
(redirect-to-stderr)=
## Redirecting Ray logs to stderr

By default, Ray logs are written to files under the ``/tmp/ray/session_*/logs`` directory. If you wish to redirect logs to stderr of the host nodes instead, you can do so by ensuring that the ``RAY_LOG_TO_STDERR=1`` environment variable is set on the driver and on all Ray nodes. This practice is not recommended but may be useful if your log processing tool needs log records to be written to stderr in order for them to be captured.

Redirecting logging to stderr will also cause a ``({component})`` prefix, e.g. ``(raylet)``, to be added to each of the log record messages.

```bash
[2022-01-24 19:42:02,978 I 1829336 1829336] (gcs_server) grpc_server.cc:103: GcsServer server started, listening on port 50009.
[2022-01-24 19:42:06,696 I 1829415 1829415] (raylet) grpc_server.cc:103: ObjectManager server started, listening on port 40545.
2022-01-24 19:42:05,087 INFO (dashboard) dashboard.py:95 -- Setup static dir for dashboard: /mnt/data/workspace/ray/python/ray/dashboard/client/build
2022-01-24 19:42:07,500 INFO (dashboard_agent) agent.py:105 -- Dashboard agent grpc address: 0.0.0.0:49228
```

This should make it easier to filter the stderr stream of logs down to the component of interest. Note that multi-line log records will **not** have this component marker at the beginning of each line.

When running a local Ray cluster, this environment variable should be set before starting the local cluster:

```python
os.environ["RAY_LOG_TO_STDERR"] = "1"
ray.init()
```

When starting a local cluster via the CLI or when starting nodes in a multi-node Ray cluster, this environment variable should be set before starting up each node:

```bash
env RAY_LOG_TO_STDERR=1 ray start
```

If using the Ray cluster launcher, you would specify this environment variable in the Ray start commands:

```bash
head_start_ray_commands:
    - ray stop
    - env RAY_LOG_TO_STDERR=1 ray start --head --port=6379 --object-manager-port=8076 --autoscaling-config=~/ray_bootstrap_config.yaml

worker_start_ray_commands:
    - ray stop
    - env RAY_LOG_TO_STDERR=1 ray start --address=$RAY_HEAD_IP:6379 --object-manager-port=8076
```

When connecting to the cluster, be sure to set the environment variable before connecting:

```python
os.environ["RAY_LOG_TO_STDERR"] = "1"
ray.init(address="auto")
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
