Using Ray From Outside the Cluster (EXPERIMENTAL)
=================================================

Getting Started
---------------

This document provides an overview and examples of how to access a ``ray`` cluster from an external client (i.e., a client on a different network).

Besides convenience, we present an example use case `here <https://docs.google.com/document/d/1iVCrHBQeF4Xq5HqFOVSDTWLDtnnjpi0LaQEttkAI7fI/edit>`_ along with an overview of how the feature is designed.

To get started, first start a ``ray`` cluster with the following command:

.. code-block:: bash
  
  ray start --head --with-gateway --use-raylet --redis-port=21216

This will start the ``ray`` cluster with a ``gateway`` process that will proxy commands and data to and from the external client. In our example, we will need to open three ports for our external client to access the cluster:

* ``21216``: this is the ``redis`` port
* ``5001``: this is the port ``socat`` (similar to ``nc`` or ``netcat``) will use to proxy commands
* ``5002``: this is the port that will transmit data

In future implementations, we will reduce this to a single port that needs to be opened.

On our client machine (e.g., a laptop), we can run the following in a python script:

.. code-block:: python

  import ray
  ray.init(redis_address="[REDIS_ADDRESS]:21216", gateway_socat_port=5001, gateway_data_port=5002, use_raylet=True)

And begin using ``ray`` as we typically would if were ``ssh``-ed into the cluster.
