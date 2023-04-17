Tune Client API
===============

You can interact with an ongoing experiment with the Tune Client API. The Tune Client API is organized around REST,
which includes resource-oriented URLs, accepts form-encoded requests, returns JSON-encoded responses,
and uses standard HTTP protocol.

To allow Tune to receive and respond to your API calls, you have to start your experiment with ``tune.run(server_port)``:

.. code-block:: python

    tune.run(..., server_port=4321)

The easiest way to use the Tune Client API is with the built-in TuneClient. To use TuneClient,
verify that you have the ``requests`` library installed:

.. code-block:: bash

    $ pip install requests

Then, on the client side, you can use the following class. If on a cluster, you may want to forward this port
(e.g. ``ssh -L <local_port>:localhost:<remote_port> <address>``) so that you can use the Client on your local machine.

.. autoclass:: ray.tune.web_server.TuneClient
    :members:

For an example notebook for using the Client API, see the
`Client API Example <https://github.com/ray-project/ray/tree/master/python/ray/tune/TuneClient.ipynb>`__.

The API also supports curl. Here are the examples for getting trials (``GET /trials/[:id]``):

.. code-block:: bash

    $ curl http://<address>:<port>/trials
    $ curl http://<address>:<port>/trials/<trial_id>

And stopping a trial (``PUT /trials/:id``):

.. code-block:: bash

    $ curl -X PUT http://<address>:<port>/trials/<trial_id>
