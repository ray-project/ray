.. _api-stability:

API stability
=============

Ray provides stability guarantees for its public APIs in Ray core and libraries, which are decorated/labeled accordingly.

An API can be labeled:

* :ref:`PublicAPI <public-api-def>`, which means the API is exposed to end users. PublicAPI has three sub-levels (alpha, beta, stable), as described below.
* :ref:`DeveloperAPI <developer-api-def>`, which means the API is explicitly exposed to *advanced* Ray users and library developers
* :ref:`Deprecated <deprecated-api-def>`, which may be removed in future releases of Ray.

Ray's PublicAPI stability definitions are based off the `Google stability level guidelines <https://google.aip.dev/181>`_, with minor differences:

.. _api-stability-alpha:

Alpha
~~~~~

An *alpha* component undergoes rapid iteration with a known set of users who
**must** be tolerant of change. The number of users **should** be a
curated, manageable set, such that it is feasible to communicate with all
of them individually.

Breaking changes **must** be both allowed and expected in alpha components, and
users **must** have no expectation of stability.

.. _api-stability-beta:

Beta
~~~~

A *beta* component **must** be considered complete and ready to be declared
stable, subject to public testing.

Because users of beta components tend to have a lower tolerance of change, beta
components **should** be as stable as possible; however, the beta component
**must** be permitted to change over time. These changes **should** be minimal
but **may** include backwards-incompatible changes to beta components.

Backwards-incompatible changes **must** be made only after a reasonable
deprecation period to provide users with an opportunity to migrate their code.

Stable
~~~~~~

A *stable* component **must** be fully-supported over the lifetime of the major
API version. Because users expect such stability from components marked stable,
there **must** be no breaking changes to these components within a major version
(excluding extraordinary circumstances).

Docstrings
----------

.. _public-api-def:

.. autofunction:: ray.util.annotations.PublicAPI

.. _developer-api-def:

.. autofunction:: ray.util.annotations.DeveloperAPI

.. _deprecated-api-def:

.. autofunction:: ray.util.annotations.Deprecated

Undecorated functions can be generally assumed to not be part of the Ray public API.
