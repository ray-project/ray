Automated cluster setup and auto-scaling support (Experimental)
===============================================================

Quick start
-----------

First, ensure you have configured your AWS credentials in ``~/.aws/credentials``,
as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

Then you're ready to go. The ``aws/example.yaml`` cluster config file will
create a small cluster with a m4.large head node (on-demand), and two m4.large
`spot workers <https://aws.amazon.com/ec2/spot/>`__.
Try it out with these commands:

.. code-block:: bash

    # Create or update the cluster
    $ ray create_or_update aws/example.yaml

    # Resize the cluster without interrupting running jobs
    $ ray create_or_update aws/example.yaml --max-workers=N --sync-only

    # Teardown the cluster
    $ ray teardown aws/example.yaml
