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


To enable tracing, you must provide a tracing startup hook to set up the Tracing Provider, Remote Span Processor, and Additional Instruments. 
TODO: add more to the documentation.
