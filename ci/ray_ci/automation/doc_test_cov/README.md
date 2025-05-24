Setup

Install packages:
pip install -r requirements.txt

Ensure you have AWS credentials defined in ~/.aws/config or exported the following vars:
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN

Run script:
python main.py --bk-api-key <bk_api_token> --ray-path <absolute-path-to-ray-repo> --look-back-hours <lookback-period-for-doc-test-build>

Test Coverage results:
Test coverage results will be output to test_cov/results/test_results.txt in text file format