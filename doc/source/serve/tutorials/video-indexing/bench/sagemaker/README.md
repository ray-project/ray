---
orphan: true
---

# SageMaker Async Inference benchmark (vs Ray Serve async)

Benchmarks **Amazon SageMaker Asynchronous Inference** against our Ray Serve async
service on the **identical** workload: `video URI -> download -> ffmpeg chunk ->
SigLIP embed -> write embeddings to S3`. Same model, same GPU (`ml.g4dn.xlarge` = T4),
same load-test rate profiles. This makes it an engine-vs-engine comparison of two
queue-backed, backlog-autoscaling async-inference systems.

## Why SageMaker Async is the right comparison
It is the closest architectural twin to Ray Serve async: submit a job (payload in S3)
-> get an ID + output location immediately -> requests queue -> workers process ->
results land in S3 -> **autoscales instances on `ApproximateBacklogSizePerInstance`
(queue depth)** -> can scale to zero. We compare throughput, cost, autoscaling
responsiveness, and reliability on the same corpus.

## Prerequisites (the AWS side)
- AWS creds (account `<your-account>`, `us-west-2`) with **SageMaker + IAM + ECR + S3 +
  CloudWatch + Application Auto Scaling**. Long-lived enough to span the run.
- A **SageMaker execution role** (S3 rw on the bucket, ECR read, CloudWatch). Create one
  or pass an existing ARN to `deploy_endpoint.py`.
- **`ml.g4dn.xlarge` service quota for async endpoints** (e.g., up to 16). Often needs an
  increase request.
- Docker (to build the image).

## Files
- `inference.py` - SageMaker handler (`model_fn`/`input_fn`/`predict_fn`/`output_fn`);
  runs the same download/chunk/SigLIP-embed/store as the Ray Serve consumer.
- `Dockerfile` + `requirements.txt` - custom image = PyTorch inference DLC + ffmpeg + code.
- `build_and_push.sh` - stage `constants.py`+`utils/`+`inference.py`, build, push to ECR.
- `deploy_endpoint.py` - Model -> async EndpointConfig -> Endpoint -> backlog autoscaling.
  Default: target-tracking on `ApproximateBacklogSizePerInstance`; `--step-scaling`: two
  step policies + two backlog alarms (the fast config used for the reported numbers).
- `sm_load.py` - stage input objects in S3, then `invoke_endpoint_async` at the same
  rate profile as `bench/phased_load.py`; writes a per-request CSV (`ack_latency_ms`, `ok`).
- `sm_metrics.py` - poll CloudWatch backlog + instance count + completed-output count -> CSV.

## Run

```bash
cd doc/source/serve/tutorials/video-indexing/bench/sagemaker
# AWS creds in the environment (account <your-account>, us-west-2): `aws configure`
# or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN.

# 1. Build + push the image (stages constants.py + utils/ + inference.py, pushes to ECR).
./build_and_push.sh video-index-sm us-west-2 latest
# -> PUSHED: <acct>.dkr.ecr.us-west-2.amazonaws.com/video-index-sm:latest

# 2. Deploy the async endpoint + the fast backlog autoscaling used for the reported numbers
#    (--step-scaling: fast-out at backlog/instance >= 5, fast-in at empty backlog).
python deploy_endpoint.py \
  --image-uri <acct>.dkr.ecr.us-west-2.amazonaws.com/video-index-sm:latest \
  --role-arn  arn:aws:iam::<acct>:role/<sagemaker-exec-role> \
  --s3-bucket <your-bucket> \
  --s3-output s3://<your-bucket>/sm-bench/output/ \
  --instance-type ml.g4dn.xlarge --min-instances 1 --max-instances 4 \
  --max-concurrency-per-instance 4 --step-scaling

# 3. Collect metrics (background) + run the SAME phased flood as Ray Serve.
PROFILE="60:0:50,360:50:50,180:100:100,360:50:50,60:75:75,120:50:50,60:50:0"
python sm_metrics.py --endpoint-name video-index-sm \
  --output-bucket <your-bucket> --output-prefix sm-bench/output/ \
  --interval 15 --duration 1800 --out sm_metrics.csv &

python sm_load.py --endpoint-name video-index-sm \
  --input-bucket <your-bucket> --input-prefix sm-bench/inputs \
  --video-uri s3://<your-bucket>/video-index/input/sample.mp4 \
  --profile "$PROFILE" --out sm_load.csv
# sm_load.csv: one row per request (ack_latency_ms, ok); failed = rows with ok=False.

# 4. Teardown (stop the cost!).
python - <<'PY'
import boto3; sm=boto3.client("sagemaker","us-west-2")
for f,k in [(sm.delete_endpoint,"EndpointName"),(sm.delete_endpoint_config,"EndpointConfigName"),(sm.delete_model,"ModelName")]:
    try: f(**{k:"video-index-sm"})
    except Exception as e: print(e)
PY
```

## Metric mapping (SageMaker <-> Ray Serve)
| Ray Serve | SageMaker Async |
|---|---|
| enqueue ack success | `invoke_endpoint_async` success |
| `/queue` depth | `ApproximateBacklogSize` (CloudWatch) |
| consumer/encoder replicas | endpoint `CurrentInstanceCount` |
| indexed count (S3) | completed objects in async output prefix |
| queue-depth autoscaling policy | step policies + alarms on `ApproximateBacklogSize`(PerInstance) |

## Caveats
- **CloudWatch resolution ~1 min** -> coarser autoscaling traces than our ~3s poller.
- **Async invoke API has TPS/concurrency quotas** -> extreme rates (1000-3000/s) may be
  capped by the API, not the backend; focus SageMaker on realistic rates + the
  autoscaling/cost/throughput story (or request a TPS increase).
- **SageMaker instance-hours carry a managed premium** over raw g4dn - measure and report it.
- Autoscaling is instance-granular via Application Auto Scaling + CloudWatch alarms
  (minutes of lag), vs Ray Serve's finer/faster replica autoscaling.
