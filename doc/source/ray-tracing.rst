Tracing
=======
To help debug and monitor Ray applications, we have built in tracing into Ray libraries. 


.. note::

    Tracing is currently an experimental feature and under active development. APIs are subject to change.

Getting Started
---------------
First, install opentelemetry.
.. code-block:: bash

    pip install opentelemetry-api==1.0.0rc1
    pip install opentelemetry-sdk==1.0.0rc1


To enable tracing, you must provide a tracing startup hook with a function that will set up the Tracer Provider, Remote Span Processors, and Additional Instruments. 

Tracer Provider
~~~~~~~~~~~~~~~~
This configures how to collect traces. View the TracerProvider API `here <https://open-telemetry.github.io/opentelemetry-python/sdk/trace.html#opentelemetry.sdk.trace.TracerProvider>`.

Remote Span Processors
~~~~~~~~~~~~~~~~~~~~~~
This configures where to export traces. View the SpanProcessor API `here <https://open-telemetry.github.io/opentelemetry-python/sdk/trace.html#opentelemetry.sdk.trace.SpanProcessor>`.

Users who want to experiment with tracing can configure their remote span processors to export spans to a local JSON file. Serious users developing locally can push their traces to Jaeger containers via the `Jaeger exporter <https://open-telemetry.github.io/opentelemetry-python/exporter/jaeger/jaeger.html>`.


Additional Instruments
~~~~~~~~~~~~~~~~~~~~~~
If you are using a library that has built-in tracing support, the setup_tracing function you provide should also patch those libraries. You can find more documentation for the instrumentation of these libraries `here <https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation>`. 


Below is an example tracing startup hook that sets up the default Tracing Provider, exports spans to files in /tmp/spans, and does not have any Additional Instruments.
.. code-block:: python

  import ray
  import os
  from opentelemetry import trace
  from opentelemetry.sdk.trace import TracerProvider
  from opentelemetry.sdk.trace.export import (
      ConsoleSpanExporter,
      SimpleExportSpanProcessor,
  )
  from typing import Any
  
  
  def setup_tracing(*args: Any, **kwargs: Any) -> None:
      if getattr(ray, "__traced__", False):
          return
  
      ray.__traced__ = True
      # Sets the tracer_provider. This is only allowed once per execution
      # context and will log a warning if attempted multiple times.
      trace.set_tracer_provider(TracerProvider())
      trace.get_tracer_provider().add_span_processor(
          SimpleExportSpanProcessor(
              ConsoleSpanExporter(
                  out=open(f"/tmp/spans_file{os.getpid()}.json", "a")
                  )
          )
      )

    
To run your program with the tracing hook, see the following examples.

.. tabs::
  .. code-tab:: start

  $ ray start --head --tracing-startup-hook "MyLibrary:setup_tracing"
  $ python
  >>> ray.init(address="auto")


  .. code-tab:: init

  $ python
  >>> ray.init(_tracing_startup_hook="MyLibrary:setup_tracing")


Custom traces
*************
Users can easily add their own custom tracing in their programs. Within the program, get the tracer object and then call trace.get_tracer(__name__)

See below for a simple example of adding custom tracing.

.. code-block:: python
  from opentelemetry import trace

  @ray.remote
  def my_func():
      tracer = trace.get_tracer(__name__)
  
      with tracer.start_as_current_span("foo"):
          print("Hello world from OpenTelemetry Python!")
