import sys

import pytest

from ray.dashboard.modules.job.cli_utils import extract_concise_error_message

NESTED_JOB_SUBMIT_ERROR = """Request failed with status code 500: Traceback (most recent call last):
  File ".../ray/dashboard/modules/job/job_head.py", line 407, in submit_job
    resp = await job_agent_client.submit_job_internal(submit_request)
  File ".../ray/dashboard/modules/job/job_head.py", line 144, in submit_job_internal
    await self._raise_error(resp)
  File ".../ray/dashboard/modules/job/job_head.py", line 130, in _raise_error
    raise RuntimeError(...)
RuntimeError: Request failed with status code 400: Traceback (most recent call last):
  File ".../ray/dashboard/modules/job/job_agent.py", line 45, in submit_job
    submission_id = await self.get_job_manager().submit_job(
  File ".../ray/dashboard/modules/job/job_manager.py", line 555, in submit_job
    raise ValueError(
ValueError: Job with submission_id pytorch-mnist-job already exists."""


class TestExtractConciseErrorMessage:
    def test_deeply_nested_traceback(self):
        assert extract_concise_error_message(NESTED_JOB_SUBMIT_ERROR) == (
            "ValueError: Job with submission_id pytorch-mnist-job already exists."
        )

    def test_single_level_no_nesting(self):
        message = "RuntimeError: Request failed with status code 400: some error."
        assert extract_concise_error_message(message) == message

    def test_no_exception_summary_line_returns_original(self):
        message = "connection refused"
        assert extract_concise_error_message(message) == message

    def test_prose_with_colon_and_spaces_is_not_mistaken_for_summary_line(self):
        message = "Request failed with status code 500: no further detail"
        assert extract_concise_error_message(message) == message

    def test_trailing_metadata_after_summary_is_not_mistaken_for_it(self):
        # Lowercase, snake_case "key: value" lines after the real exception
        # summary must not be selected instead of it (regression test for a
        # code-review finding that flagged the original overly broad regex).
        message = (
            "ValueError: Job with submission_id my_job already exists.\n"
            "submission_id: my_job\n"
            "status: FAILED"
        )
        assert extract_concise_error_message(message) == (
            "ValueError: Job with submission_id my_job already exists."
        )

    def test_dotted_qualified_exception_name(self):
        message = (
            "requests.exceptions.ConnectionError: Failed to establish a connection."
        )
        assert extract_concise_error_message(message) == message


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
