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


Tracing is currently disabled by default, but you can enable it by setting the environment variable ['RAY_TRACING_ENABLED'] to ['"True"'].

