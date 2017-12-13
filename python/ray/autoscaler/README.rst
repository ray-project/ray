Automated cluster setup and auto-scaling support (Experimental)
===============================================================

Quick start
-----------

First, ensure you have configured your AWS credentials in ``~/.aws/credentials``,
as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

Then you're ready to go. The default cluster example file will create a small
cluster with one m4.large head node and two m4.large workers. This is enough
to get started with Ray, but for more more intensive workloads you will want to
change the instance types to e.g. use GPU or larger compute instance by editing
the yaml file. Try out the example:

.. code-block:: bash

    # To create or update the cluster
    $ ray create_or_update aws/example.yaml

    # To resize the cluster without interrupting running jobs
    $ ray create_or_update aws/example.yaml --max-workers=N --sync-only

    # To teardown the cluster
    $ ray teardown aws/example.yaml
