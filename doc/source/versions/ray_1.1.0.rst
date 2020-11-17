.. _ray1.1.0:

Ray 1.1.0 Release Notes
=======================

Ray Core
--------

- The CLI now also supports Docker. (#11761)
- Support running in Docker as a non-root user. (#11407)
- Upgrade Redis dependence from 5.0.9 to 6.0.9 (#11371)
- Upgrade Pillow dependence from 5.4.1 to 7.2.0 (#11371)

GCS
---

- Reduce the number of GET operations on the worker table. (#11599)

Autoscaler
----------

- Added callback system. (#11674)

RLlib
-----

- Trajectory view API enabled by default for PPO, IMPALA, PG, and A3C. (#11747)

