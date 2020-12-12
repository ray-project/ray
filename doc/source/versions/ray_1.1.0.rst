.. _ray1.1.0:

Ray 1.1.0 Release Notes
=======================

.. To see all the pull requests merged after 1.0.1, search
.. is:pr is:merged merged:>2020-11-09T12:50:00+00:00 sort:merged-asc
.. https://docs.github.com/en/free-pro-team@latest/github/searching-for-information-on-github/searching-issues-and-pull-requests#search-by-when-a-pull-request-was-merged
.. Of course some, such as #9749, are internal and need not be noted here.

Ray Core
--------

- The CLI now also supports Docker. (#11761)
- Support running in Docker as a non-root user. (#11407)
- Upgrade Redis dependence from 5.0.9 to 6.0.9 (#11371)
- Upgrade Pillow dependence from 5.4.1 to 7.2.0 (#11371)

Documentation
-------------

- Send users to master in preference to the latest release. (#11897)

Java
----

- Java actor classes can now inherit from other Java actor classes. #12001

Serve
-----

- ``http_host`` can now be set to None to skip starting HTTP servers. (#11627)

GCS
---

- Reduce the number of GET operations on the worker table. (#11599)

Autoscaler
----------

- Added callback system to ``create_or_update_cluster``. (#11674)
- Cache resources for speed. (#12028)
- Give ``max_workers`` precedence over ``min_workers`` in resource demand scheduler. (#12106)

RLlib
-----

- Trajectory view API enabled by default for PPO, IMPALA, PG, and A3C. (#11747)
- MAML extension now supported for all models except RNNs. (#11337)
- Add ``on_learn_on_batch`` callback by default. (#12070)

Multi-tenancy
-------------

- Deprecated ``enable_multi_tenancy`` now removed. (#10573)

