---
paths:
  - "**/*.py"
---
<!-- Python coding guidelines — derived from docs.ray.io/en/master/ray-contribute/getting-involved.html -->
<!-- Source of truth: doc/source/ray-contribute/getting-involved.rst "Code Style" section -->
- Place type annotations in function signatures so tooling (mypy, pyright) can use them directly
- Use @pytest.mark.parametrize to cover multiple cases in a single test function, reducing duplication
- Extract common test setup/teardown into reusable helper functions
- Use @classmethod setUpClass/tearDownClass to reuse Ray clusters across test suites (avoids ~4.4s startup per test)
