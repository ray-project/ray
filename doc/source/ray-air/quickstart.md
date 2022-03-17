# Ray AI Runtime

Ray AI Runtime (AIR) is an open-source toolkit for building end-to-end ML applications. By leveraging Ray and its library ecosystem, it brings scalability and programmability to ML platforms.

The main focuses of the Ray AI Runtime:

* Ray AIR focuses on providing the compute layer for ML workloads.
* It is designed to interoperate with other systems for storage and metadata needs.

```{image} images/ecosystem.svg
```

Ray AIR consists of 5 key components -- Data processing (Ray Data), Model Training (Ray Train), Reinforcement Learning (Ray RLlib), Hyperparameter Tuning (Ray Tune), and Model Serving (Ray Serve). 


Users can use these libraries interchangeably to scale different parts of standard ML workflows. 


## Ray AI Runtime Quick Start

Simply click on the dropdowns below to see examples of our most popular libraries. 

`````{dropdown} <img src="/ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px"> Data: Creating and Transforming Datasets
:animate: fade-in-slide-down

Ray Datasets are the standard way to load and exchange data in Ray libraries and applications.
Datasets provide basic distributed data transformations such as `map`, `filter`, and `repartition`.
They are compatible with a variety of file formats, datasources, and distributed frameworks.

````{note}
To get started with this example install Ray Data as follows.

```bash
pip install "ray[data]" dask
```
````

Get started by creating Datasets from synthetic data using ``ray.data.range()`` and ``ray.data.from_items()``.
Datasets can hold either plain Python objects (schema is a Python type), or Arrow records (schema is Arrow).

```{literalinclude} ../data/doc_code/quick_start.py
:language: python
:start-after: __data_setup_begin__
:end-before: __data_setup_end__
```

Datasets can be created from files on local disk or remote datasources such as S3. Any filesystem 
[supported by pyarrow](http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html) can be used to specify file locations.
You can also create a ``Dataset`` from existing data in the Ray object store or Ray-compatible distributed DataFrames:

```{literalinclude} ../data/doc_code/quick_start.py
:language: python
:start-after: __data_load_begin__
:end-before: __data_load_end__
```
Datasets can be transformed in parallel using ``.map()``. 
Transformations are executed *eagerly* and block until the operation is finished.
Datasets also supports ``.filter()`` and ``.flat_map()``.

```{literalinclude} ../data/doc_code/quick_start.py
:language: python
:start-after: __data_transform_begin__
:end-before: __data_transform_end__
```

```{link-button} ../data/dataset
:type: ref
:text: Learn more about Ray Data
:classes: btn-outline-primary btn-block
```
`````

`````{dropdown} <img src="/ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px"> Train: Distributed Model Training
:animate: fade-in-slide-down

Ray Train abstracts away the complexity of setting up a distributed training
system. Let's take following simple examples:

````{tabbed} PyTorch

This example shows how you can use Ray Train with PyTorch.

First, set up your dataset and model.

```{literalinclude} /../../python/ray/train/examples/torch_quick_start.py
:language: python
:start-after: __torch_setup_begin__
:end-before: __torch_setup_end__
```

Now define your single-worker PyTorch training function.

```{literalinclude} /../../python/ray/train/examples/torch_quick_start.py
:language: python
:start-after: __torch_single_begin__
:end-before: __torch_single_end__
```

This training function can be executed with:

```{literalinclude} /../../python/ray/train/examples/torch_quick_start.py
:language: python
:start-after: __torch_single_run_begin__
:end-before: __torch_single_run_end__
```

Now let's convert this to a distributed multi-worker training function!

All you have to do is use the ``ray.train.torch.prepare_model`` and
``ray.train.torch.prepare_data_loader`` utility functions to
easily setup your model & data for distributed training.
This will automatically wrap your model with ``DistributedDataParallel``
and place it on the right device, and add ``DistributedSampler`` to your DataLoaders.

```{literalinclude} /../../python/ray/train/examples/torch_quick_start.py
:language: python
:start-after: __torch_distributed_begin__
:end-before: __torch_distributed_end__
```

Then, instantiate a ``Trainer`` that uses a ``"torch"`` backend
with 4 workers, and use it to run the new training function!

```{literalinclude} /../../python/ray/train/examples/torch_quick_start.py
:language: python
:start-after: __torch_trainer_begin__
:end-before: __torch_trainer_end__
```
````

````{tabbed} TensorFlow

This example shows how you can use Ray Train to set up `Multi-worker training
with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_.

First, set up your dataset and model.

```{literalinclude} /../../python/ray/train/examples/tensorflow_quick_start.py
:language: python
:start-after: __tf_setup_begin__
:end-before: __tf_setup_end__
```

Now define your single-worker TensorFlow training function.

```{literalinclude} /../../python/ray/train/examples/tensorflow_quick_start.py
:language: python
:start-after: __tf_single_begin__
:end-before: __tf_single_end__
```

This training function can be executed with:

```{literalinclude} /../../python/ray/train/examples/tensorflow_quick_start.py
:language: python
:start-after: __tf_single_run_begin__
:end-before: __tf_single_run_end__
```

Now let's convert this to a distributed multi-worker training function!
All you need to do is:

1. Set the *global* batch size - each worker will process the same size
   batch as in the single-worker code.
2. Choose your TensorFlow distributed training strategy. In this example
   we use the ``MultiWorkerMirroredStrategy``.

```{literalinclude} /../../python/ray/train/examples/tensorflow_quick_start.py
:language: python
:start-after: __tf_distributed_begin__
:end-before: __tf_distributed_end__
```

Then, instantiate a ``Trainer`` that uses a ``"tensorflow"`` backend
with 4 workers, and use it to run the new training function!

```{literalinclude} /../../python/ray/train/examples/tensorflow_quick_start.py
:language: python
:start-after: __tf_trainer_begin__
:end-before: __tf_trainer_end__
```
````

```{link-button} ../train/train
:type: ref
:text: Learn more about Ray Train
:classes: btn-outline-primary btn-block
```
`````

`````{dropdown} <img src="/ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px"> Tune: Hyperparameter Tuning at Scale
:animate: fade-in-slide-down

[Tune](../tune/index.rst) is a library for hyperparameter tuning at any scale. 
With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code. 
Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

````{note}
To run this example, you will need to install the following:

```bash
pip install "ray[tune]"
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

```{link-button} ../tune/index
:type: ref
:text: Learn more about Ray Tune
:classes: btn-outline-primary btn-block
```

`````


`````{dropdown} <img src="/ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px"> Serve: Scalable Model Serving
:animate: fade-in-slide-down

[Ray Serve](../serve/index) is a scalable model-serving library built on Ray. 

````{note}
To run this example, you will need to install the following libraries.

```{code-block} bash
pip install "ray[serve]" scikit-learn
```
````
This example runs serves a scikit-learn gradient boosting classifier.

```{literalinclude} ../serve/_examples/doc_code/quick_start.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

As a result you will see `{"result": "versicolor"}`.

```{link-button} ../serve/index
:type: ref
:text: Learn more about Ray Serve
:classes: btn-outline-primary btn-block
```
`````


`````{dropdown} <img src="/ray-overview/images/ray_svg_logo.svg" alt="ray" width="50px"> RLlib: Industry-Grade Reinforcement Learning
:animate: fade-in-slide-down

[RLlib](../rllib/index.rst) is an industry-grade library for reinforcement learning (RL) built on top of Ray.
RLlib offers high scalability and unified APIs for a variety of industry- and research applications.

````{note}
To run this example, you will need to install `rllib` and either `tensorflow` or `pytorch`.
```bash
pip install "ray[rllib]" tensorflow  # or torch
```
````

```{literalinclude} ../../../rllib/examples/documentation/rllib_on_ray_readme.py
:end-before: __quick_start_end__
:language: python
:start-after: __quick_start_begin__
```

```{link-button} ../rllib/index
:type: ref
:text: Learn more about Ray RLlib
:classes: btn-outline-primary btn-block
```

`````
## Why Ray AIR?

We believe Ray AIR provides unique value deriving from Ray.

**Reduced development friction**: Ray AIR reduces development friction going from development to production. Unlike in other frameworks, scaling Ray applications from a laptop to large clusters doesn't require a separate way of running -- the same code scales up seamlessly.

This means that data scientists and ML practitioners spend less time fighting YAMLs and refactoring code. Smaller teams and companies that don’t have the resources to invest heavily on MLOps can now deploy ML models at a much faster rate with Ray AIR.

**Multi-cloud and framework-interoperable**: Ray AIR is multi-cloud and framework-interoperable. The Ray compute layer and libraries freely operate with Cloud platforms and frameworks in the ecosystem, reducing lock-in to any particular choices of ML tech.

Here's an example why framework interoperability is unique to Ray -- it's easy to run Torch distributed or elastic Horovod within Ray, but not vice versa. 

**Future-proof**: Ray's scalability and flexibility makes Ray AIR future-proof. Advanced serving pipelines, elastic training, online learning, reinforcement learning applications are being built and scaled today on Ray. Common patterns are being incorporated into libraries like Serve.


## What is Ray AIR not? 

Ray AIR focuses on the compute-intensive portions of the stack. Storage and model registries are not part of Ray AIR, but over time, Ray AIR will provide integrations with data sources and metadata registries like MLFlow and WandB.
