.. meta::
   :description: The OpenAPI specification for the Ray Jobs REST API, with every endpoint, request body, and response schema.

.. _ray-job-rest-api-openapi:

Ray Jobs REST API specification
===============================

This page documents every endpoint of the Ray Jobs REST API, generated from the
:ref:`OpenAPI specification <ray-job-rest-api-spec>`. For request examples and a
walkthrough, see :ref:`ray-job-rest-api`.

.. Request and response examples are deliberately not enabled here, and can't be
   until two upstream bugs are fixed. sphinxcontrib-openapi's
   :generate-examples-from-schemas: crashes on this spec with
   "ValueError: dictionary update sequence element #0 has length 1; 2 is
   required" (sphinx-contrib/openapi#166), because example generation can't
   handle an allOf that targets a non-object schema, which is how JobDetails
   attaches descriptions to the JobType and JobStatus string enums. The
   :response-examples-for: option is rejected outright as an unknown option
   (sphinx-contrib/openapi#165). Re-check both before adding either option.

.. openapi:: openapi.yml
