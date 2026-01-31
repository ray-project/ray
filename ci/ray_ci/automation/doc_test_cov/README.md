This script determines the test coverage for all doc files in the doc/* path

The script queries recent postmerge buildkite builds run by release-automation and gets the jobs ids for all doctest related tests

The branch (master), commit and jobs ids are used to download the bazel logs for these jobs from s3 storage

Once downloaded, the test status is parsed for each bazel target & doc file.

A summary of the results are then saved to results/test_results.txt with the test coverage results included

# Setup

## Install depedencies
pip install -r requirements.txt

Ensure you have AWS credentials defined in ~/.aws/config or exported as the following vars:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN

To set these vars navigate to go/aws and search for the anyscale-prod-osstravistracker AWS account
Click Access Keys and copy the snippet under `Option 1: Set AWS environment variables`

## Setup buildkite api token
Go to https://buildkite.com/user/api-access-tokens and setup an API access token with the following options checked:
read_builds

## Build the docs locally (in doc/)

To build the documentation, make sure you have `ray` installed first.
For building the documentation locally install the following dependencies:

```bash
pip install -r requirements.txt
```

### Building the documentation

To compile the documentation and open it locally, run the following command from this directory.

```bash
make develop && open _build/html/index.html
```

## Run script
python main.py --bk-api-token <bk_api_token> --ray-path <absolute-path-to-ray-repo> --look-back-hours <lookback-period-for-doc-test-build>

## Test Coverage results:
Test coverage results will be output to `doc_test_cov/results/json` in json file format and 
`doc_test_cov/results/csv` in the csv file format

Test coverage percentage will be output to std out once the script is complete

CSV format for the test coverage results can be imported to google sheets and filtered for further analysis