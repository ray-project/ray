.. _ray-tracing:

Tracing
=======
To help debug and monitor Ray applications, Ray integrates with OpenTelemetry to make it easy to export traces to external tracing stacks such as Jaeger. 


.. note::

    Tracing is currently an experimental feature and under active development. APIs are subject to change.

Getting Started
---------------
First, install OpenTelemetry.

.. code-block:: shell

    pip install opentelemetry-api==1.1.0
    pip install opentelemetry-sdk==1.1.0
    pip install opentelemetry-exporter-otlp==1.1.0

Tracing Startup Hook
--------------------
To enable tracing, you must provide a tracing startup hook with a function that will set up the :ref:`Tracer Provider <tracer-provider>`, :ref:`Remote Span Processors <remote-span-processors>`, and :ref:`Additional Instruments <additional-instruments>`. The tracing startup hook is expected to be a function that will be called with no args or kwargs. This hook needs to be available in the Python environment of all the worker processes.

Below is an example tracing startup hook that sets up the default tracing provider, exports spans to files in ``/tmp/spans``, and does not have any additional instruments.
 
.. code-block:: python

  import ray
  import os
  from opentelemetry import trace
  from opentelemetry.sdk.trace import TracerProvider
  from opentelemetry.sdk.trace.export import (
      ConsoleSpanExporter,
      SimpleSpanProcessor,
  )
  
  
  def setup_tracing() -> None:
      # Creates /tmp/spans folder
      os.makedirs("/tmp/spans", exist_ok=True)
      # Sets the tracer_provider. This is only allowed once per execution
      # context and will log a warning if attempted multiple times.
      trace.set_tracer_provider(TracerProvider())
      trace.get_tracer_provider().add_span_processor(
          SimpleSpanProcessor(
              ConsoleSpanExporter(
                  out=open(f"/tmp/spans/{os.getpid()}.json", "a")
                  )
          )
      )


For open-source users who want to experiment with tracing, Ray has a default tracing startup hook that exports spans to the folder ``/tmp/spans``. To run using this default hook, you can run the following code sample to set up tracing and trace a simple Ray task.

.. tab-set::

    .. tab-item:: ray start

        .. code-block:: shell

            $ ray start --head --tracing-startup-hook=ray.util.tracing.setup_local_tmp_tracing:setup_tracing
            $ python
            >>> ray.init()
            >>> @ray.remote
                def my_function():
                    return 1

                obj_ref = my_function.remote()

    .. tab-item:: ray.init()

        .. code-block:: python

            >>> ray.init(_tracing_startup_hook="ray.util.tracing.setup_local_tmp_tracing:setup_tracing")
            >>> @ray.remote
                def my_function():
                    return 1

                obj_ref = my_function.remote()

If you want to provide your own custom tracing startup hook, provide your startup hook in the format of ``module:attribute`` where the attribute is the ``setup_tracing`` function to be run.

.. _tracer-provider:

Tracer Provider
~~~~~~~~~~~~~~~
This configures how to collect traces. View the TracerProvider API `here <https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html#opentelemetry.sdk.trace.TracerProvider>`__.

.. _remote-span-processors:

Remote Span Processors
~~~~~~~~~~~~~~~~~~~~~~
This configures where to export traces to. View the SpanProcessor API `here <https://opentelemetry-python.readthedocs.io/en/latest/sdk/trace.html#opentelemetry.sdk.trace.SpanProcessor>`__.

Users who want to experiment with tracing can configure their remote span processors to export spans to a local JSON file. Serious users developing locally can push their traces to Jaeger containers via the `Jaeger exporter <https://opentelemetry-python.readthedocs.io/en/latest/exporter/jaeger/jaeger.html#module-opentelemetry.exporter.jaeger>`_.

.. _additional-instruments:

Additional Instruments
~~~~~~~~~~~~~~~~~~~~~~
If you are using a library that has built-in tracing support, the ``setup_tracing`` function you provide should also patch those libraries. You can find more documentation for the instrumentation of these libraries `here <https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation>`_.

Custom Traces
*************
You can easily add custom tracing in your programs. Within your program, get the tracer object with ``trace.get_tracer(__name__)`` and start a new span with ``tracer.start_as_current_span(...)``.

See below for a simple example of adding custom tracing.

.. code-block:: python

  from opentelemetry import trace

  @ray.remote
  def my_func():
      tracer = trace.get_tracer(__name__)

      with tracer.start_as_current_span("foo"):
          print("Hello world from OpenTelemetry Python!")
