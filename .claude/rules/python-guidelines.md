---
paths:
  - "**/*.py"
---
<!-- Python coding guidelines — derived from docs.ray.io/en/master/ray-contribute/getting-involved.html -->
<!-- Source of truth: doc/source/ray-contribute/getting-involved.rst "Code Style" section -->
- Follow Black code style format (enforced via ruff)
- Line length: 88 chars
- Imports follow PEP8 conventions; add imports at top of files
- Docstrings use Google format: first sentence on same line as opening quotes, must fit one line
- Type information belongs in function signatures, not in docstrings
- Prefer @pytest.mark.parametrize over duplicated test functions
- Extract common test setup/teardown into reusable helper functions
- Async tests require pytest-asyncio and @pytest.mark.asyncio
- Use @classmethod setUpClass/tearDownClass to reuse Ray clusters across test suites (avoids ~4.4s startup per test)
- Be careful with parallel test runners (pytest-xdist) — Ray services can timeout if started concurrently
