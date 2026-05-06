.. _serve-dns-aid:

DNS-AID Deployment Discovery
============================

Ray Serve can publish a `DNS-AID <https://dns-aid.org>`_ SVCB record
(per :rfc:`9460`) for every deployment that reaches **HEALTHY** state.
External AI agents can then discover Serve deployments via a standard
DNS lookup — no Ray dashboard access, no GCS credentials required.

Overview
--------

DNS-AID uses DNS SVCB "ServiceMode" records under an ``_agents`` subdomain
tree to advertise agent endpoints. Ray Serve creates one record per named
deployment following the convention::

    _{deployment_name}._a2a._agents.{zone}

The SVCB target points to the Serve HTTP proxy ingress (host + port 8000
by default). The record carries additional **SvcParams** hints that encode
the deployment's route prefix, current target replica count, and application
name so clients can make routing decisions without an extra API call.

Quick-start
-----------

1. **Install the optional dependency**::

       pip install "ray[serve]" dns-aid

2. **Configure via environment variables** (zero-code-change path)::

       export DNS_AID_ENABLED=1
       export DNS_AID_ZONE=agents.example.com   # your DNS zone
       export DNS_AID_SERVER=ns1.example.com    # DNS server that accepts updates

3. **Start Serve normally**. Records are published automatically as
   deployments reach HEALTHY state and deregistered on deletion or
   permanent unhealthy.

Enabling via ServeDeploySchema
------------------------------

If you manage Serve via the REST API or a config file, set
``dns_aid_enabled: true`` in your deploy config:

.. code-block:: yaml

   proxy_location: EveryNode
   dns_aid_enabled: true
   http_options:
     host: "0.0.0.0"
     port: 8000
   applications:
     - name: my_app
       route_prefix: /hello
       import_path: my_module:app

Or programmatically:

.. code-block:: python

   import json
   import requests

   config = {
       "dns_aid_enabled": True,
       "applications": [
           {"name": "my_app", "route_prefix": "/hello",
            "import_path": "my_module:app"},
       ],
   }
   requests.put("http://localhost:52365/api/serve/applications/",
                json=config)

Enabling via DnsAidConfig
-------------------------

Pass a :class:`~ray.serve.config.DnsAidConfig` when starting the
ServeController directly (advanced / embedded use-cases):

.. code-block:: python

   from ray.serve.config import DnsAidConfig
   # ServeController.__init__ accepts dns_aid_config as a keyword argument
   dns_aid_config = DnsAidConfig(
       enabled=True,
       zone="agents.example.com",
       server="ns1.example.com",
       ttl=30,
   )

Configuration reference
-----------------------

All settings can be supplied via environment variables (picked up
automatically by :class:`~ray.serve.config.DnsAidConfig`) or set
explicitly in code.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Env variable
     - Default
     - Description
   * - ``DNS_AID_ENABLED``
     - ``0``
     - Set to ``1`` / ``true`` / ``yes`` to enable.  This env var is
       an **operator-level override** that takes priority over
       ``dns_aid_enabled`` in ``ServeDeploySchema``.
   * - ``DNS_AID_ZONE``
     - *(required)*
     - DNS zone where records are published, e.g. ``agents.example.com``.
   * - ``DNS_AID_SERVER``
     - *(required)*
     - Hostname or IP of the DNS server that accepts dynamic updates.
   * - ``DNS_AID_PORT``
     - ``53``
     - Port of the DNS server.

Record format
-------------

Each record follows the SVCB ServiceMode format defined in :rfc:`9460`.

.. code-block:: text

    _{deployment_name}._a2a._agents.{zone}  IN SVCB 1 {ingress_host} (
        alpn="h2"
        ... )

**SvcParams hints** carried on every record:

.. list-table::
   :header-rows: 1

   * - Hint key
     - Example value
     - Description
   * - ``route``
     - ``/my-deployment``
     - The deployment's ``route_prefix``.
   * - ``replicas``
     - ``4``
     - Target replica count at the time the record was last updated.
   * - ``framework``
     - ``ray-serve``
     - Always ``ray-serve``; useful for filtering in multi-framework zones.
   * - ``app_name``
     - ``my_app``
     - The Serve application name that owns this deployment.

Discovery example
-----------------

Any agent that understands DNS-AID can discover Serve deployments:

.. code-block:: python

   from dns_aid import discover_agents_via_dns

   agents = discover_agents_via_dns("agents.example.com")
   for agent in agents:
       print(agent.name, "→", agent.target, ":", agent.port)
       print("  route:", agent.params.hints.get("route"))
       print("  app:  ", agent.params.hints.get("app_name"))

Behaviour and guarantees
------------------------

* **Opt-in** — all registration code is a no-op (zero overhead) unless
  ``dns_aid_enabled=True`` or ``DNS_AID_ENABLED=1``.
* **Non-blocking** — DNS I/O is scheduled as a fire-and-forget
  ``asyncio.Task`` from the ServeController event loop.
* **Best-effort** — registration and deregistration errors are logged
  at WARNING level but never propagate to the controller.
* **Recovery-safe** — records are only published after the controller has
  finished recovering from a checkpoint, preventing stale records.
* **Per-deployment granularity** — each named deployment gets its own
  SVCB record; multiple deployments in the same application each
  receive an independent record.

Troubleshooting
---------------

No records appearing
    * Check ``DNS_AID_ENABLED`` is set correctly.
    * Verify ``DNS_AID_ZONE`` matches your DNS zone.
    * Look for ``DNS-AID:`` log lines in the ServeController log.

Records point to loopback address
    * The Serve HTTP proxy was configured with ``host: 127.0.0.1`` or
      ``0.0.0.0``. Serve replaces these with the head-node's public IP
      automatically, so check that ``ray.util.get_node_ip_address()``
      returns the expected public address on your cluster.

``dns-aid`` import errors
    * Install the package: ``pip install dns-aid``.
    * The package is optional; missing it suppresses registration silently
      (DEBUG log line emitted).
