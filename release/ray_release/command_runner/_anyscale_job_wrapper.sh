#!/bin/bash
# $1 - test workload, eg. python workloads/script.py
# $2 - bucket address to upload results.json to
# $3 - bucket address to upload metrics.json to

set -x

start_time=$(date +%s)

# 1. Run the test workload
$1
return_code=$?

end_time=$(date +%s)
time_taken=$(( end_time - start_time ))

echo "Finished with return code $return_code, time taken $time_taken"

# 2. Install awscli
pip install -q awscli

# 3. Upload results.json to s3
aws s3 cp "$TEST_OUTPUT_JSON" "$2" --acl bucket-owner-full-control

# 4. Collect metrics

# Logic duplicated in ray_release/glue.py:346
# Calculate dynamic timeout
# Timeout is the time the test took divided by 200
# (~7 minutes for a 24h test) but no less than 30s
# and no more than 900s
timeout_time=$(( time_taken / 200 ))
if (( timeout_time > 900 )); then
    timeout_time=900
    elif (( timeout_time < 90 )); then
    timeout_time=30
fi
timeout_time=800
timeout --verbose "$timeout_time" python prometheus_metrics.py "$start_time" --path "$METRICS_OUTPUT_JSON"

# 5. Upload metrics.json to s3
aws s3 cp "$METRICS_OUTPUT_JSON" "$3" --acl bucket-owner-full-control

exit "$return_code"