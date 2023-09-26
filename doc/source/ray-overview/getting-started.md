(gentle-intro)=

# Getting Started
Use Ray to scale applications on your laptop or the cloud. Choose the right guide for your task.
* Scale ML workloads: [Ray Libraries Quickstart](#libraries-quickstart)
* Scale general Python applications: [Ray Core Quickstart](#ray-core-quickstart)
* Deploy to the cloud: [Ray Clusters Quickstart](#ray-cluster-quickstart)
* Debug and monitor applications: [Debugging and Monitoring Quickstart](#debugging-and-monitoring-quickstart)


(libraries-quickstart)=
## Ray AI Libraries Quickstart

Use individual libraries for ML workloads. Click on the dropdowns for your workload below.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Data: Scalable Datasets for ML
:animate: fade-in-slide-down

Scale offline inference and training ingest with [Ray Data](data_key_concepts) --
a data processing library designed for ML.

To learn more, see [Offline batch inference](batch_inference_overview) and
[Data preprocessing and ingest for ML training](ml_ingest_overview).

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

Ray Train abstracts away the complexity of setting up a distributed training
system.

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
:dedent: 0
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
:dedent: 0
```
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

[Tune](../tune/index.rst) is a library for hyperparameter tuning at any scale.
With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code.
Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

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

If TensorBoard is installed, automatically visualize all trial results:

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

[Ray Serve](../serve/index) is a scalable model-serving library built on Ray.

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

As a result you will see `{"result": "versicolor"}`.

```{button-ref}  ../serve/index
:color: primary
:outline:
:expand:

Learn more about Ray Serve
```

`````


`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> RLlib: Industry-Grade Reinforcement Learning
:animate: fade-in-slide-down

[RLlib](../rllib/index.rst) is an industry-grade library for reinforcement learning (RL) built on top of Ray.
RLlib offers high scalability and unified APIs for a variety of industry- and research applications.

````{note}
To run this example, install `rllib` and either `tensorflow` or `pytorch`:

```bash
pip install -U "ray[rllib]" tensorflow  # or torch
```
````

```{literalinclude} ../../../rllib/examples/documentation/rllib_on_ray_readme.py
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

Turn functions and classes easily into Ray tasks and actors,
for Python and Java, with simple primitives for building and running distributed applications.


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
        // Intialize Ray runtime.
        Ray.init();
        List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
        // Invoke the `square` method 4 times remotely as Ray tasks.
        // The tasks will run in parallel in the background.
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
When you instantiate a class that is a Ray actor, Ray will start a remote instance
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
        // Intialize Ray runtime.
        Ray.init();
        List<ActorHandle<Counter>> counters = new ArrayList<>();
        // Create 4 actors from the `Counter` class.
        // They will run in remote worker processes.
        for (int i = 0; i < 4; i++) {
            counters.add(Ray.actor(Counter::new).remote());
        }

        // Invoke the `increment` method on each actor.
        // This will send an actor task to each remote actor.
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

Deploy your applications on Ray clusters, often with minimal code changes to your existing code.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Clusters: Launching a Ray Cluster on AWS
:animate: fade-in-slide-down

Ray programs can run on a single machine, or seamlessly scale to large clusters.
Take this simple example that waits for individual nodes to join the cluster.

````{dropdown} example.py
:animate: fade-in-slide-down

```{literalinclude} ../../yarn/example.py
:language: python
```
````
You can also download this example from our [GitHub repository](https://github.com/ray-project/ray/blob/master/doc/yarn/example.py).
Go ahead and store it locally in a file called `example.py`.

To execute this script in the cloud, just download [this configuration file](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml),
or copy it here:

````{dropdown} cluster.yaml
:animate: fade-in-slide-down

```{literalinclude} ../../../python/ray/autoscaler/aws/example-full.yaml
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

Learn more about launching Ray Clusters
```

`````

## Debugging and Monitoring Quickstart

Use built-in observability tools to monitor and debug Ray applications and clusters.


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
Access the dashboard through the default URL, http://localhost:8265.

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

See the summarized statistics of Ray tasks using ``ray summary tasks``.

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

```{include} learn-more.md
```
