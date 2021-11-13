.. _serve-pipeline-api:

Pipeline API (Experimental)
===========================

This section should help you:

- understand the experimental pipeline API
- build on top of the API to construct your multi-model inference pipelines.


.. note::
    This API is experimental and the API is subject to change.
    We are actively looking for feedback via the Ray Slack or Github issues.

Basic API
---------

Serve Pipeline is a standalone package that can be used without Ray Serve. 
However, the expected usage is to use it inside your Serve deployment.

You can import it as:

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __import_start__
    :end-before: __import_end__

You can decorate any function or classes using ``pipeline.step``. You should
then pass it's upstream input by calling the decorated function or class. In
the example below, we have a single step that takes the special node ``pipeline.INPUT``.

Once you created the pipeline node, you can call ``.deploy()`` to instantiate the whole pipeline.
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
upstream nodes. 

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __class_node_start__
    :end-before: __class_node_end__

The decorator also takes two arguments to configure where the node will be executed.

.. autofunction:: ray.serve.pipeline.step

Here is an example pipeline that uses actors instead of local execution mode.

.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __pipeline_configuration_start__
    :end-before: __pipeline_configuration_end__


Chaining Example
----------------

In this section, we show how to implement a two stage pipeline that's common
for computer vision task. For workload like image classification, the preprocessing
steps are CPU bounded and hard to parallelize. The actual inference steps can run
on GPU and batched. They are often split up into separate stages and scaled separately
to increase throughput.


.. literalinclude:: ../../../python/ray/serve/examples/doc/snippet_pipeline.py
    :language: python
    :start-after: __preprocessing_pipeline_example_start__
    :end-before: __preprocessing_pipeline_example_end__


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

