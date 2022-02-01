.. _serve-pipeline-api:

Pipeline API (Experimental)
===========================

This section should help you:

- understand the experimental pipeline API.
- build on top of the API to construct your multi-model inference pipelines.


.. note::
    This API is experimental and the API is subject to change.
    We are actively looking for feedback via the Ray `Forum`_ or `GitHub Issues`_

Serve Pipeline is a new experimental package purposely built to help developing
and deploying multi-models inference pipelines, also known as model composition.

Model composition is common in real-world ML applications. In many cases, you need to:

- Split CPU bounded preprocessing and GPU bounded model inference to scale each phase separately.
- Chain multiple models together for a single tasks.
- Combine the output from multiple models to create ensemble result.
- Dynamically select models based on attribute of the input data.

The Serve Pipeline has the following features:

- It has a python based, declarative API for constructing pipeline DAG.
- It gives you visibility into the whole pipeline without losing the flexibility
  of coding arbitrary graph using code.
- You can develop and test pipeline locally with local execution mode.
- Each model in the DAG can be scaled to many replicas across the Ray cluster.
  You can fine-tune the resource usage to achieve maximum throughput and utilization.

Compare to ServeHandle, Serve Pipeline is more explicit about the dependencies
of each model in the pipeline and let you deploy the entire DAG at once.

Compare to KServe (formerly KFServing), Serve Pipeline enables writing pipeline
as code and arbitrary control flow operation using Python.

Compare to building your own orchestration micro-services, Serve Pipeline helps
you to be productive in building scalable pipeline in hours.


Basic API
---------

Serve Pipeline is a standalone package that can be used without Ray Serve.
However, the expected usage is to use it inside your Serve deployment.

You can import it as:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __import_start__
    :end-before: __import_end__

You can decorate any function or class using ``pipeline.step``. You can then
combine these steps into a pipeline by calling the decorated steps. In
the example below, we have a single step that takes the special node ``pipeline.INPUT``,
, which is a placeholder for the arguments that will be passed into the pipeline.

Once you have defined the pipeline by combining one or more steps, you can call ``.deploy()`` to instantiate it.
Once you have instantiated the pipeline, you can call ``.call(inp)`` to invoke synchronously.

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __simple_pipeline_start__
    :end-before: __simple_pipeline_end__

The input to a pipeline node can be the ``pipeline.INPUT`` special node or
one or more other pipeline nodes. Here is an example of simple chaining pipeline.

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __simple_chain_start__
    :end-before: __simple_chain_end__

For classes, you need to instantiate them with init args first, then pass in their
upstream nodes. This allows you to have the same code definition but pass different
arguments, like URIs for model weights (you can see an example of this in the
:ref:`ensemble example <serve-pipeline-ensemble-api>` section.)

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __class_node_start__
    :end-before: __class_node_end__

The decorator also takes two arguments to configure where the node will be executed.

.. autofunction:: ray.serve.pipeline.step

Here is an example pipeline that uses actors instead of local execution mode. The local
execution mode is the default running mode. It runs the node directly within the process
instead of distributing them out. This mode is useful for local testing and development.

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __pipeline_configuration_start__
    :end-before: __pipeline_configuration_end__


Chaining Example
----------------

In this section, we show how to implement a two stage pipeline that's common
for computer vision tasks. For workloads like image classification, the preprocessing
steps are CPU bounded and hard to parallelize. The actual inference steps can run
on GPU and batched (batching helps improving throughput without sacrificing latency,
you can learn more in our :ref:`batching tutorial <serve-batch-tutorial>`).
They are often split up into separate stages and scaled separately to increase throughput.


.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __preprocessing_pipeline_example_start__
    :end-before: __preprocessing_pipeline_example_end__


.. _serve-pipeline-ensemble-api:


Ensemble Example
----------------

We will now expand on previous example to construct an ensemble pipeline. In
the previous example, our pipeline looks like: preprocess -> resnet18. What if we
want to aggregate the output from many different models? You can build this scatter-gather
pattern easily with Pipeline. The below code snippet shows how to construct a pipeline
looks like: preprocess -> [resnet18, resnet34] -> combine_output.


.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __ensemble_pipeline_example_start__
    :end-before: __ensemble_pipeline_example_end__

More Use Case Examples
----------------------

There are even more use cases for Serve Pipeline.

.. note::
    Please feel free to suggest more use cases and contribute your examples by
    sending a `Github Pull Requests`_!

Combining business logic + ML models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Based off the previous ensemble example, you can put arbitrary business logic
in your pipeline step.

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __biz_logic_start__
    :end-before: __biz_logic_end__



.. _`Forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`GitHub Pull Requests`: https://github.com/ray-project/ray/pulls
