Security
========

This document describes best security practices for using Ray.

Intended Use and Threat Model
-----------------------------

Ray instances should run on a secure network without public facing ports.
The most common threat for Ray instances is unauthorized access to Redis,
which can be exploited to gain shell access and run arbitray code.
The best fix is to run Ray instances on a secure, trusted network.

Running Ray on a secured network is not always feasible, so Ray
provides some basic security features:


Redis Port Authentication
-------------------------

To prevent exploits via unauthorized Redis access, Ray provides the option to
password-protect Redis ports. While this is not a replacement for running Ray
behind a firewall, this feature is useful for instances exposed to the internet
where configuring a firewall is not possible. Because Redis is
very fast at serving queries, the chosen password should be long.

Redis authentication is only supported on the raylet code path.

To add authentication via the Python API, start Ray using:

.. code-block:: python

  ray.init(redis_password="password")

To add authentication via the CLI, or connect to an existing Ray instance with
password-protected Redis ports:

.. code-block:: bash

  ray start [--head] --redis-password="password"

While Redis port authentication may protect against external attackers,
Ray does not encrypt traffic between nodes so man-in-the-middle attacks are
possible for clusters on untrusted networks.

Cloud Security
--------------

Launching Ray clusters on AWS or GCP using the ``ray up`` command
automatically configures security groups that prevent external Redis access.

References
----------

- The `Redis security documentation <https://redis.io/topics/security>`
