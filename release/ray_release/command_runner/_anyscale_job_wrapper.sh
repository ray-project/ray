#!/bin/bash
# $1 - test workload, eg. python workloads/script.py
# $2 - test workload timeout (set to <0 for infinite)
# $3 - is test long running (don't fail on timeout)
# $4 - bucket address to upload results.json to
# $5 - bucket address to upload metrics.json to

test_workload="$1"
test_workload_timeout="$2"
is_test_workload_long_running="$3"
results_s3_path="$4"
metrics_s3_path="$5"

set -x

start_time=$(date +%s)

# 1. Run the test workload

echo "$test_workload" > "workload.sh"
chmod +x workload.sh
cat workload.sh

if (( test_workload_timeout < 0 )); then
    bash -x ./workload.sh
else
    timeout --verbose "$test_workload_timeout" bash -x ./workload.sh
fi
return_code=$?
exit_return_code="$return_code"

if [ "$is_test_workload_long_running" == "true" ] && [ "$return_code" == "124" ]; then
    exit_return_code=0
fi

end_time=$(date +%s)
time_taken=$(( end_time - start_time ))

echo "Finished with return code $return_code, time taken $time_taken"

# 2. Install awscli
pip install -q awscli

# 3. Upload results.json to s3
aws s3 cp "$TEST_OUTPUT_JSON" "$results_s3_path" --acl bucket-owner-full-control

# 4. Collect metrics

# Logic duplicated in ray_release/glue.py:346
# Calculate dynamic timeout
# Timeout is the time the test took divided by 200
# (~7 minutes for a 24h test) but no less than 90s
# and no more than 900s
timeout_time=$(( time_taken / 200 ))
if (( timeout_time > 900 )); then
    timeout_time=900
    elif (( timeout_time < 90 )); then
    timeout_time=90
fi
timeout --verbose "$timeout_time" python prometheus_metrics.py "$start_time" --path "$METRICS_OUTPUT_JSON"

# 5. Upload metrics.json to s3
aws s3 cp "$METRICS_OUTPUT_JSON" "$metrics_s3_path" --acl bucket-owner-full-control

# 6. Use job outputs for return code
pip install -U anyscale
python -c "import anyscale.job;anyscale.job.output({'return_code':$return_code,'exit_return_code':$exit_return_code})"

echo "$exit_return_code"
exit "$exit_return_code"