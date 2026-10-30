---
myst:
  html_meta:
    description: "The OpenAPI specification for the Ray Jobs REST API, with every endpoint, request body, and response schema."
---

(ray-job-rest-api-openapi)=

# Ray Jobs REST API specification

This page documents every endpoint of the Ray Jobs REST API, generated from the [OpenAPI specification](ray-job-rest-api-spec). For request examples and a walkthrough, see [](ray-job-rest-api).

% Request and response examples are deliberately not enabled here, and can't be until an upstream bug is fixed. With :generate-examples-from-schemas:, sphinxcontrib-openapi renders a schema-derived string example without JSON-encoding it, so the stop endpoint's 200 response, a bare JobStatus enum, renders as PENDING instead of "PENDING" under Content-Type: application/json. The http lexer rejects it, and the warning fails the build (sphinx-contrib/openapi#172). The fix for the httpdomain renderer is in sphinx-contrib/openapi#185, which isn't in a release yet. Re-check before adding the option.

% The openapi directive has to be wrapped in eval-rst. sphinxcontrib-openapi emits
% reStructuredText into a ViewList and hands it to nested_parse, but MyST's
% nested_parse renders that content as Markdown. A bare ```{openapi}``` fence
% therefore builds green with zero warnings and silently degrades the whole page:
% every ".. http:get::" and ":resjson:" line renders as literal text, so there are
% no httpdomain objects, no field lists, no anchors, and no search or
% cross-referencing. eval-rst parses the content with a real rST parser and
% produces output identical to the .rst page this replaced.

```{eval-rst}
.. openapi:: openapi.yml
```
