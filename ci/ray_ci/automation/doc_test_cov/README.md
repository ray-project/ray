This script determines the test coverage for all doc files in the doc/* path

The script queries recent postmerge buildkite builds run by release-automation and gets the jobs ids for all doctest related tests

The branch (master), commit and jobs ids are used to download the bazel logs for these jobs from s3 storage

Once downloaded, the test status is parsed for each bazel target & doc file.

A summary of the results are then saved to results/test_results.txt with the test coverage results included

Setup

Install depedencies:
pip install -r requirements.txt

Ensure you have AWS credentials defined in ~/.aws/config or exported the following vars:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN

Setup buildkite api token:
Go to https://buildkite.com/user/api-access-tokens and setup an API access token with the following options checked:
read_builds

Run script:
python main.py --bk-api-token <bk_api_token> --ray-path <absolute-path-to-ray-repo> --look-back-hours <lookback-period-for-doc-test-build>

Test Coverage results:
Test coverage results will be output to test_cov/results/test_results.txt in text file format