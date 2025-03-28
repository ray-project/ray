(gentle-intro)=

# Getting Started

Ray is an open source unified framework for scaling AI and Python applications. It provides a simple, universal API for building distributed applications that can scale from a laptop to a cluster.

## What's Ray?

Ray simplifies distributed computing by providing:
- **Scalable compute primitives**: Tasks and actors for painless parallel programming
- **Specialized AI libraries**: Tools for common ML workloads like data processing, model training, hyperparameter tuning, and model serving
- **Unified resource management**: Seamless scaling from laptop to cloud with automatic resource handling

## Choose Your Path

Select the guide that matches your needs:
* **Scale ML workloads**: [Ray Libraries Quickstart](#libraries-quickstart)
* **Scale general Python applications**: [Ray Core Quickstart](#ray-core-quickstart)
* **Deploy to the cloud**: [Ray Clusters Quickstart](#ray-cluster-quickstart)
* **Debug and monitor applications**: [Debugging and Monitoring Quickstart](#debugging-and-monitoring-quickstart)

```{image} ../images/map-of-ray.svg
:align: center
:alt: Ray Framework Architecture
```

(libraries-quickstart)=
## Ray AI Libraries Quickstart

Use individual libraries for ML workloads. Each library specializes in a specific part of the ML workflow, from data processing to model serving. Click on the dropdowns for your workload below.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Data: Scalable Datasets for ML
:animate: fade-in-slide-down

[Ray Data](data_quickstart) provides distributed data processing optimized for machine learning and AI workloads. It efficiently streams data through data pipelines.

Here's an example on how to scale offline inference and training ingest with Ray Data.

````{note}
To run this example, install Ray Data:

```bash
pip install -U "ray[data]"
```
````

```{testcode}
from typing import Dict
import numpy as np
import ray

# Create datasets from on-disk files, Python objects, and cloud storage like S3.
ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

# Apply functions to transform data. Ray Data executes transformations in parallel.
def compute_area(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    length = batch["petal length (cm)"]
    width = batch["petal width (cm)"]
    batch["petal area (cm^2)"] = length * width
    return batch

transformed_ds = ds.map_batches(compute_area)

# Iterate over batches of data.
for batch in transformed_ds.iter_batches(batch_size=4):
    print(batch)

# Save dataset contents to on-disk files or cloud storage.
transformed_ds.write_parquet("local:///tmp/iris/")
```

```{testoutput}
:hide:

...
```

```{button-ref}  ../data/data
:color: primary
:outline:
:expand:

Learn more about Ray Data
```
`````

``````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Train: Distributed Model Training
:animate: fade-in-slide-down

**Ray Train** makes distributed model training simple. It abstracts away the complexity of setting up distributed training across popular frameworks like PyTorch and TensorFlow.

`````{tab-set}

````{tab-item} PyTorch

This example shows how you can use Ray Train with PyTorch.

To run this example install Ray Train and PyTorch packages:

:::{note}
```bash
pip install -U "ray[train]" torch torchvision
```
:::

Set up your dataset and model.

```{literalinclude} /../../python/ray/train/examples/pytorch/torch_quick_start.py
:language: python
:start-after: __torch_setup_begin__
:end-before: __torch_setup_end__
```

Now define your single-worker PyTorch training function.

```{literalinclude} /../../python/ray/train/examples/pytorch/torch_quick_start.py
:language: python
:start-after: __torch_single_begin__
:end-before: __torch_single_end__
```

This training function can be executed with:

```{literalinclude} /../../python/ray/train/examples/pytorch/torch_quick_start.py
:language: python
:start-after: __torch_single_run_begin__
:end-before: __torch_single_run_end__
:dedent: 4
```

Convert this to a distributed multi-worker training function.

Use the ``ray.train.torch.prepare_model`` and
``ray.train.torch.prepare_data_loader`` utility functions to
set up your model and data for distributed training.
This automatically wraps the model with ``DistributedDataParallel``
and places it on the right device, and adds ``DistributedSampler`` to the DataLoaders.

```{literalinclude} /../../python/ray/train/examples/pytorch/torch_quick_start.py
:language: python
:start-after: __torch_distributed_begin__
:end-before: __torch_distributed_end__
```

Instantiate a ``TorchTrainer``
with 4 workers, and use it to run the new training function.

```{literalinclude} /../../python/ray/train/examples/pytorch/torch_quick_start.py
:language: python
:start-after: __torch_trainer_begin__
:end-before: __torch_trainer_end__
:dedent: 4
```

To accelerate the training job using GPU, make sure you have GPU configured, then set `use_gpu` to `True`. If you don't have a GPU environment, Anyscale provides a development workspace integrated with an autoscaling GPU cluster for this purpose.

<div class="anyscale-cta">
    <a href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=get-started-train-torch">
        <img src="../_static/img/try-ray-on-anyscale.svg" alt="Try Ray on Anyscale">
    </a>
</div>

````


````{tab-item} TensorFlow

This example shows how you can use Ray Train to set up [Multi-worker training
with Keras](https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras).

To run this example install Ray Train and Tensorflow packages:

:::{note}
```bash
pip install -U "ray[train]" tensorflow
```
:::

Set up your dataset and model.

```{literalinclude} /../../python/ray/train/examples/tf/tensorflow_quick_start.py
:language: python
:start-after: __tf_setup_begin__
:end-before: __tf_setup_end__
```

Now define your single-worker TensorFlow training function.

```{literalinclude} /../../python/ray/train/examples/tf/tensorflow_quick_start.py
:language: python
:start-after: __tf_single_begin__
:end-before: __tf_single_end__
```

This training function can be executed with:

```{literalinclude} /../../python/ray/train/examples/tf/tensorflow_quick_start.py
:language: python
:start-after: __tf_single_run_begin__
:end-before: __tf_single_run_end__
:dedent: 0
```

Now convert this to a distributed multi-worker training function.

1. Set the *global* batch size - each worker processes the same size
   batch as in the single-worker code.
2. Choose your TensorFlow distributed training strategy. This examples
   uses the ``MultiWorkerMirroredStrategy``.

```{literalinclude} /../../python/ray/train/examples/tf/tensorflow_quick_start.py
:language: python
:start-after: __tf_distributed_begin__
:end-before: __tf_distributed_end__
```

Instantiate a ``TensorflowTrainer``
with 4 workers, and use it to run the new training function.

```{literalinclude} /../../python/ray/train/examples/tf/tensorflow_quick_start.py
:language: python
:start-after: __tf_trainer_begin__
:end-before: __tf_trainer_end__
:dedent: 0
```

To accelerate the training job using GPU, make sure you have GPU configured, then set `use_gpu` to `True`. If you don't have a GPU environment, Anyscale provides a development workspace integrated with an autoscaling GPU cluster for this purpose.

<div class="anyscale-cta">
    <a href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=get-started-train-tf">
        <img src="../_static/img/try-ray-on-anyscale.svg" alt="Try Ray on Anyscale">
    </a>
</div>

```{button-ref}  ../train/train
:color: primary
:outline:
:expand:

Learn more about Ray Train
```

````

`````

``````

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Tune: Hyperparameter Tuning at Scale
:animate: fade-in-slide-down

[Ray Tune](../tune/index.rst) is a library for hyperparameter tuning at any scale.
It automatically finds the best hyperparameters for your models with efficient distributed search algorithms.
With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code, supporting any deep learning framework including PyTorch, TensorFlow, and Keras.

````{note}
To run this example, install Ray Tune:

```bash
pip install -U "ray[tune]"
```
````

This example runs a small grid search with an iterative training function.

```{literalinclude} ../../../python/ray/tune/tests/example.py
:end-before: __quick_start_end__
:language: python
:start-after: __quick_start_begin__
```

If TensorBoard is installed (`pip install tensorboard`), you can automatically visualize all trial results:

```bash
tensorboard --logdir ~/ray_results
```

```{button-ref}  ../tune/index
:color: primary
:outline:
:expand:

Learn more about Ray Tune
```

`````


`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Serve: Scalable Model Serving
:animate: fade-in-slide-down

[Ray Serve](../serve/index) provides scalable and programmable serving for ML models and business logic. Deploy models from any framework with production-ready performance.

````{note}
To run this example, install Ray Serve and scikit-learn:

```{code-block} bash
pip install -U "ray[serve]" scikit-learn
```
````

This example runs serves a scikit-learn gradient boosting classifier.

```{literalinclude} ../serve/doc_code/sklearn_quickstart.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

The response shows `{"result": "versicolor"}`.

```{button-ref}  ../serve/index
:color: primary
:outline:
:expand:

Learn more about Ray Serve
```

`````


`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> RLlib: Industry-Grade Reinforcement Learning
:animate: fade-in-slide-down

[RLlib](../rllib/index.rst) is a reinforcement learning (RL) library that offers high performance implementations of popular RL algorithms and supports various training environments. RLlib offers high scalability and unified APIs for a variety of industry- and research applications.

````{note}
To run this example, install `rllib` and either `tensorflow` or `pytorch`:

```bash
pip install -U "ray[rllib]" tensorflow  # or torch
```
You may also need CMake installed on your system.

````

```{literalinclude} ../rllib/doc_code/rllib_on_ray_readme.py
:end-before: __quick_start_end__
:language: python
:start-after: __quick_start_begin__
```

```{button-ref}  ../rllib/index
:color: primary
:outline:
:expand:

Learn more about Ray RLlib
```

`````

## Ray Core Quickstart

<a href="https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-core-quickstart&redirectTo=/v2/template-preview/workspace-intro">
    <img src="../_static/img/run-on-anyscale.svg" alt="try-anyscale-quickstart-ray-quickstart">
</a>
<br></br>

Ray Core provides simple primitives for building and running distributed applications. It enables you to turn regular Python or Java functions and classes into distributed stateless tasks and stateful actors with just a few lines of code.

The examples below show you how to:
1. Convert Python functions to Ray tasks for parallel execution
2. Convert Python classes to Ray actors for distributed stateful computation


``````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Core: Parallelizing Functions with Ray Tasks
:animate: fade-in-slide-down

`````{tab-set}

````{tab-item} Python

:::{note}
To run this example install Ray Core:

```bash
pip install -U "ray"
```
:::

Import Ray and and initialize it with `ray.init()`.
Then decorate the function with ``@ray.remote`` to declare that you want to run this function remotely.
Lastly, call the function with ``.remote()`` instead of calling it normally.
This remote call yields a future, a Ray _object reference_, that you can then fetch with ``ray.get``.

```{code-block} python

import ray
ray.init()

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]
```

````

````{tab-item} Java

```{note}
To run this example, add the [ray-api](https://mvnrepository.com/artifact/io.ray/ray-api) and [ray-runtime](https://mvnrepository.com/artifact/io.ray/ray-runtime) dependencies in your project.
```

Use `Ray.init` to initialize Ray runtime.
Then use `Ray.task(...).remote()` to convert any Java static method into a Ray task.
The task runs asynchronously in a remote worker process. The `remote` method returns an ``ObjectRef``,
and you can fetch the actual result with ``get``.

```{code-block} java

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;

public class RayDemo {

    public static int square(int x) {
        return x * x;
    }

    public static void main(String[] args) {
        // Initialize Ray runtime.
        Ray.init();
        List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
        // Invoke the `square` method 4 times remotely as Ray tasks.
        // The tasks run in parallel in the background.
        for (int i = 0; i < 4; i++) {
            objectRefList.add(Ray.task(RayDemo::square, i).remote());
        }
        // Get the actual results of the tasks.
        System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
    }
}
```

In the above code block we defined some Ray Tasks. While these are great for stateless operations, sometimes you
must maintain the state of your application. You can do that with Ray Actors.

```{button-ref}  ../ray-core/walkthrough
:color: primary
:outline:
:expand:

Learn more about Ray Core
```

````

`````

``````

``````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Core: Parallelizing Classes with Ray Actors
:animate: fade-in-slide-down

Ray provides actors to allow you to parallelize an instance of a class in Python or Java.
When you instantiate a class that is a Ray actor, Ray starts a remote instance
of that class in the cluster. This actor can then execute remote method calls and
maintain its own internal state.

`````{tab-set}

````{tab-item} Python

:::{note}
To run this example install Ray Core:

```bash
pip install -U "ray"
```
:::

```{code-block} python

import ray
ray.init() # Only call this once.

@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n

counters = [Counter.remote() for i in range(4)]
[c.increment.remote() for c in counters]
futures = [c.read.remote() for c in counters]
print(ray.get(futures)) # [1, 1, 1, 1]
```
````

````{tab-item} Java


```{note}
To run this example, add the [ray-api](https://mvnrepository.com/artifact/io.ray/ray-api) and [ray-runtime](https://mvnrepository.com/artifact/io.ray/ray-runtime) dependencies in your project.
```

```{code-block} java

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RayDemo {

    public static class Counter {

        private int value = 0;

        public void increment() {
            this.value += 1;
        }

        public int read() {
            return this.value;
        }
    }

    public static void main(String[] args) {
        // Initialize Ray runtime.
        Ray.init();
        List<ActorHandle<Counter>> counters = new ArrayList<>();
        // Create 4 actors from the `Counter` class.
        // These run in remote worker processes.
        for (int i = 0; i < 4; i++) {
            counters.add(Ray.actor(Counter::new).remote());
        }

        // Invoke the `increment` method on each actor.
        // This sends an actor task to each remote actor.
        for (ActorHandle<Counter> counter : counters) {
            counter.task(Counter::increment).remote();
        }
        // Invoke the `read` method on each actor, and print the results.
        List<ObjectRef<Integer>> objectRefList = counters.stream()
            .map(counter -> counter.task(Counter::read).remote())
            .collect(Collectors.toList());
        System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
    }
}
```

```{button-ref}  ../ray-core/walkthrough
:color: primary
:outline:
:expand:

Learn more about Ray Core
```

````

`````

``````

## Ray Cluster Quickstart

Deploy your applications on Ray clusters on AWS, GCP, Azure, and more, often with minimal code changes to your existing code.


`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Clusters: Launching a Ray Cluster on AWS
:animate: fade-in-slide-down

Ray programs can run on a single machine, or seamlessly scale to large clusters.

:::{note}
To run this example install the following:

```bash
pip install -U "ray[default]" boto3
```

If you haven't already, configure your credentials as described in the [documentation for boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#guide-credentials).
:::

Take this simple example that waits for individual nodes to join the cluster.

````{dropdown} example.py
:animate: fade-in-slide-down

```{literalinclude} ../../yarn/example.py
:language: python
```
````
You can also download this example from the [GitHub repository](https://github.com/ray-project/ray/blob/master/doc/yarn/example.py).
Store it locally in a file called `example.py`.

To execute this script in the cloud, download [this configuration file](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-minimal.yaml),
or copy it here:

````{dropdown} cluster.yaml
:animate: fade-in-slide-down

```{literalinclude} ../../../python/ray/autoscaler/aws/example-minimal.yaml
:language: yaml
```
````

Assuming you have stored this configuration in a file called `cluster.yaml`, you can now launch an AWS cluster as follows:

```bash
ray submit cluster.yaml example.py --start
```

```{button-ref}  cluster-index
:color: primary
:outline:
:expand:

Learn more about launching Ray Clusters on AWS, GCP, Azure, and more
```

`````

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Clusters: Launching a Ray Cluster on Kubernetes
:animate: fade-in-slide-down

Ray programs can run on a single node Kubernetes cluster, or seamlessly scale to larger clusters.

```{button-ref}  kuberay-index
:color: primary
:outline:
:expand:

Learn more about launching Ray Clusters on Kubernetes
```

`````

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Clusters: Launching a Ray Cluster on Anyscale 
:animate: fade-in-slide-down

Anyscale is the company behind Ray. The Anyscale platform provides an enterprise-grade Ray deployment on top of your AWS, GCP, Azure, or on-prem Kubernetes clusters.

```{button-link} https://www.anyscale.com/ray-on-anyscale?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=get-started-launch-ray-cluster
:color: primary
:outline:
:expand:

Try Ray on Anyscale
```

`````

## Debugging and Monitoring Quickstart

Use built-in observability tools to monitor and debug Ray applications and clusters. These tools help you understand your application's performance and identify bottlenecks.


`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Ray Dashboard: Web GUI to monitor and debug Ray
:animate: fade-in-slide-down

Ray dashboard provides a visual interface that displays real-time system metrics, node-level resource monitoring, job profiling, and task visualizations. The dashboard is designed to help users understand the performance of their Ray applications and identify potential issues.

```{image} https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/Dashboard-overview.png
:align: center
```

````{note}
To get started with the dashboard, install the default installation as follows:

```bash
pip install -U "ray[default]"
```
````
The dashboard automatically becomes available when running Ray scripts. Access the dashboard through the default URL, http://localhost:8265.

```{button-ref}  observability-getting-started
:color: primary
:outline:
:expand:

Learn more about Ray Dashboard
```

`````

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Ray State APIs: CLI to access cluster states
:animate: fade-in-slide-down

Ray state APIs allow users to conveniently access the current state (snapshot) of Ray through CLI or Python SDK.

````{note}
To get started with the state API, install the default installation as follows:

```bash
pip install -U "ray[default]"
```
````

Run the following code.

```{code-block} python
import ray
import time

ray.init(num_cpus=4)

@ray.remote
def task_running_300_seconds():
    print("Start!")
    time.sleep(300)

@ray.remote
class Actor:
    def __init__(self):
        print("Actor created")

# Create 2 tasks
tasks = [task_running_300_seconds.remote() for _ in range(2)]

# Create 2 actors
actors = [Actor.remote() for _ in range(2)]

ray.get(tasks)
```

See the summarized statistics of Ray tasks using ``ray summary tasks`` in a terminal.

```{code-block} bash
ray summary tasks
```

```{code-block} text
======== Tasks Summary: 2022-07-22 08:54:38.332537 ========
Stats:
------------------------------------
total_actor_scheduled: 2
total_actor_tasks: 0
total_tasks: 2


Table (group by func_name):
------------------------------------
FUNC_OR_CLASS_NAME        STATE_COUNTS    TYPE
0   task_running_300_seconds  RUNNING: 2      NORMAL_TASK
1   Actor.__init__            FINISHED: 2     ACTOR_CREATION_TASK
```

```{button-ref}  observability-programmatic
:color: primary
:outline:
:expand:

Learn more about Ray State APIs
```

`````

## Learn More

Ray has a rich ecosystem of resources to help you learn more about distributed computing and AI scaling.

### Blog and Press

- [Modern Parallel and Distributed Python: A Quick Tutorial on Ray](https://towardsdatascience.com/modern-parallel-and-distributed-python-a-quick-tutorial-on-ray-99f8d70369b8)
- [Why Every Python Developer Will Love Ray](https://www.datanami.com/2019/11/05/why-every-python-developer-will-love-ray/)
- [Ray: A Distributed System for AI (Berkeley Artificial Intelligence Research, BAIR)](http://bair.berkeley.edu/blog/2018/01/09/ray/)
- [10x Faster Parallel Python Without Python Multiprocessing](https://towardsdatascience.com/10x-faster-parallel-python-without-python-multiprocessing-e5017c93cce1)
- [Implementing A Parameter Server in 15 Lines of Python with Ray](https://ray-project.github.io/2018/07/15/parameter-server-in-fifteen-lines.html)
- [Ray Distributed AI Framework Curriculum](https://rise.cs.berkeley.edu/blog/ray-intel-curriculum/)
- [RayOnSpark: Running Emerging AI Applications on Big Data Clusters with Ray and Analytics Zoo](https://medium.com/riselab/rayonspark-running-emerging-ai-applications-on-big-data-clusters-with-ray-and-analytics-zoo-923e0136ed6a)
- [First user tips for Ray](https://rise.cs.berkeley.edu/blog/ray-tips-for-first-time-users/)
- [Tune: a Python library for fast hyperparameter tuning at any scale](https://towardsdatascience.com/fast-hyperparameter-tuning-at-scale-d428223b081c)
- [Cutting edge hyperparameter tuning with Ray Tune](https://medium.com/riselab/cutting-edge-hyperparameter-tuning-with-ray-tune-be6c0447afdf)
- [New Library Targets High Speed Reinforcement Learning](https://www.datanami.com/2018/02/01/rays-new-library-targets-high-speed-reinforcement-learning/)
- [Scaling Multi Agent Reinforcement Learning](http://bair.berkeley.edu/blog/2018/12/12/rllib/)
- [Functional RL with Keras and Tensorflow Eager](https://bair.berkeley.edu/blog/2019/10/14/functional-rl/)
- [How to Speed up Pandas by 4x with one line of code](https://www.kdnuggets.com/2019/11/speed-up-pandas-4x.html)
- [Quick Tip—Speed up Pandas using Modin](https://pythondata.com/quick-tip-speed-up-pandas-using-modin/)
- [Ray Blog](https://medium.com/distributed-computing-with-ray)

### Videos

- [Unifying Large Scale Data Preprocessing and Machine Learning Pipelines with Ray Data \| PyData 2021](https://zoom.us/rec/share/0cjbk_YdCTbiTm7gNhzSeNxxTCCEy1pCDUkkjfBjtvOsKGA8XmDOx82jflHdQCUP.fsjQkj5PWSYplOTz?startTime=1635456658000) [(slides)](https://docs.google.com/presentation/d/19F_wxkpo1JAROPxULmJHYZd3sKryapkbMd0ib3ndMiU/edit?usp=sharing)
- [Programming at any Scale with Ray \| SF Python Meetup Sept 2019](https://www.youtube.com/watch?v=LfpHyIXBhlE)
- [Ray for Reinforcement Learning \| Data Council 2019](https://www.youtube.com/watch?v=Ayc0ca150HI)
- [Scaling Interactive Pandas Workflows with Modin](https://www.youtube.com/watch?v=-HjLd_3ahCw)
- [Ray: A Distributed Execution Framework for AI \| SciPy 2018](https://www.youtube.com/watch?v=D_oz7E4v-U0)
- [Ray: A Cluster Computing Engine for Reinforcement Learning Applications \| Spark Summit](https://www.youtube.com/watch?v=xadZRRB_TeI)
- [RLlib: Ray Reinforcement Learning Library \| RISECamp 2018](https://www.youtube.com/watch?v=eeRGORQthaQ)
- [Enabling Composition in Distributed Reinforcement Learning \| Spark Summit 2018](https://www.youtube.com/watch?v=jAEPqjkjth4)
- [Tune: Distributed Hyperparameter Search \| RISECamp 2018](https://www.youtube.com/watch?v=38Yd_dXW51Q)


### Slides

- [Talk given at UC Berkeley DS100](https://docs.google.com/presentation/d/1sF5T_ePR9R6fAi2R6uxehHzXuieme63O2n_5i9m7mVE/edit?usp=sharing)
- [Talk given in October 2019](https://docs.google.com/presentation/d/13K0JsogYQX3gUCGhmQ1PQ8HILwEDFysnq0cI2b88XbU/edit?usp=sharing)
- [Talk given at RISECamp 2019](https://docs.google.com/presentation/d/1v3IldXWrFNMK-vuONlSdEuM82fuGTrNUDuwtfx4axsQ/edit?usp=sharing)


### Papers

-   [Ray 2.0 Architecture white paper](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview)
-   [Ray 1.0 Architecture white paper (old)](https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview)
-   [Exoshuffle: large-scale data shuffle in Ray](https://arxiv.org/abs/2203.05072)
-   [RLlib paper](https://arxiv.org/abs/1712.09381)
-   [RLlib flow paper](https://arxiv.org/abs/2011.12719)
-   [Tune paper](https://arxiv.org/abs/1807.05118)
-   [Ray paper (old)](https://arxiv.org/abs/1712.05889)
-   [Ray HotOS paper (old)](https://arxiv.org/abs/1703.03924)

If you encounter technical issues, post on the [Ray discussion forum](https://discuss.ray.io/). For general questions, announcements, and community discussions, join the [Ray community on Slack](https://www.ray.io/join-slack).