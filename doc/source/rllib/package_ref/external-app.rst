External Application API
------------------------

In some cases, for instance when interacting with an externally hosted simulator or
production environment, it makes more sense to interact with RLlib as if it were an
independently running service, rather than RLlib hosting the simulations itself.
This is possible via RLlib's external applications interface
`(full documentation) <rllib-env.html#external-agents-and-applications>`__.

.. autoclass:: ray.rllib.env.policy_client.PolicyClient
    :members:

.. autoclass:: ray.rllib.env.policy_server_input.PolicyServerInput
    :members:

.. include:: /_includes/rllib/announcement_bottom.rst
