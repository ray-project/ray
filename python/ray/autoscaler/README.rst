Cluster setup and auto-scaling support (Experimental)
=====================================================

Quick start
-----------

First, modify the example.json file to include your AWS ssh keypair.

::
    # To create or update the cluster
    $ ray bootstrap aws/example.json

    # To teardown the cluster
    $ ray teardown aws/example.json
