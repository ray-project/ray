Automated cluster setup and auto-scaling support (Experimental)
===============================================================

Quick start
-----------

First, modify the example.json file to include your AWS ssh keypair.

.. code-block:: bash

    # To create or update the cluster
    $ ray create_or_update aws/example.json

    # To teardown the cluster
    $ ray teardown aws/example.json
