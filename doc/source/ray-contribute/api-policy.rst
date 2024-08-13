.. _api-policy:

API Policy
=============

Ray APIs refer to classes, class methods, or functions.
When we declare an API, we promise our
users that they can use these APIs to develop their
apps without worrying about changes to these
interfaces between different Ray releases. Declaring
or deprecating an API has a significant impact on the
community.  This document proposes simple policies to
hold Ray contributors accountable to these promises
and manage user expectations.

For API exposure levels, see :ref:`API Stability <api-stability>`.


API documentation policy
~~~~~~~~~~~~~~~~~~~~~~~~
Documentation is one of the main channels through which
we expose our APIs to users. If we provide incorrect
information, it can significantly impact the reliability
and maintainability of our users' applications. Based on
the API exposure level, here is the policy to ensure the
accuracy of our information.

.. list-table:: API Documentation Policy
    :widths: 20 16 16 16 16 16
    :header-rows: 1

    * - Policy/Exposure Level
      - Stable Public API
      - Beta Public API
      - Alpha Public API
      - Deprecated
      - Developer API
    * - Must this API be documented?
      - Yes
      - Yes
      - Yes
      - Yes
      - Up to the developers
    * - Must a method be annotated with one of the API annotations (PublicAPI, DeveloperAPI or Deprecated)?
      - Yes
      - Yes
      - Yes
      - Yes
      - No. The absence of annotations implies the Developer API level by default.
    * - Can this API be private (either inside the _internal module or has an underscore prefix)?
      - No
      - No
      - No
      - No
      - No

API Lifecycle Policy
~~~~~~~~~~~~~~~~~~~~
Users have high expectations for certain exposure levels,
so we need to be cautious when moving APIs between different
levels. Here is the policy for managing the API exposure lifecycle.

.. list-table:: API Lifecycle Policy
    :widths: 20 16 16 16 16 16
    :header-rows: 1

    * - Policy/Exposure Level
      - Stable Public API
      - Beta Public API
      - Alpha Public API
      - Deprecated API
      - Developer API
    * - Can this API be promoted to a higher level without any warnings, heads up to users?
      - Yes
      - Yes
      - Yes
      - No
      - Yes
    * - Can this API be demoted to a lower level? If so then how?
      - Can be demoted to Deprecated only. The API should emit warning messages and a deadline for deprecations in **6 months (or +25 ray minor versions)**.
      - Can be demoted to Deprecated only. The API should emit warning messages and a deadline for deprecations in **3 months (or +12 ray minor versions)**.
      - Users must allow for and expect breaking changes in alpha components, and must have no expectations of stability.
      - Yes
      - No annotations mean it is a developer API by default
    * - Can you remove or change this API's parameters?
      - Yes. The API should emit warning messages and you must set a deadline for the end-of-life of the original version that is **6 months or +25 Ray minor versions**. During the transition period, you must support both the new and old parameters.
      - Yes. The API should emit warning messages and you must set a deadline for the change in **3 months or +12 Ray minor versions**. During the transition period, you must support both the new and old parameters.
      - Users must allow for and expect breaking changes in alpha components, and must have no expectations of stability.
      - No
      - Yes
