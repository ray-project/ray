---
myst:
  html_meta:
    description: "The OpenAPI specification for the Ray Jobs REST API, with every endpoint, request body, and response schema."
---

(ray-job-rest-api-openapi)=

# Ray Jobs REST API specification

This page documents every endpoint of the Ray Jobs REST API, generated from the [OpenAPI specification](ray-job-rest-api-spec). For request examples and a walkthrough, see [](ray-job-rest-api).

% Request and response examples are deliberately not enabled here, and can't be until two upstream bugs are fixed. sphinxcontrib-openapi's :generate-examples-from-schemas: crashes on this spec with "ValueError: dictionary update sequence element #0 has length 1; 2 is required" (sphinx-contrib/openapi#166), because example generation can't handle an allOf that targets a non-object schema, which is how JobDetails attaches descriptions to the JobType and JobStatus string enums. The :response-examples-for: option is rejected outright as an unknown option (sphinx-contrib/openapi#165). Re-check both before adding either option.

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
