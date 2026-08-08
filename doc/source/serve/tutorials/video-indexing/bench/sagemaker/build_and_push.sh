#!/usr/bin/env bash
# Stage the shared project code, build the SageMaker inference image, push to ECR.
# Needs valid AWS creds (ECR + Docker). Usage:
#   build_and_push.sh [ECR_REPO] [REGION] [TAG]
set -euo pipefail
REPO="${1:-video-index-sm}"
REGION="${2:-us-west-2}"
TAG="${3:-latest}"
D="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$(cd "$D/../.." && pwd)"   # video-indexing/

# 1. Stage code/ = constants.py + utils/ + inference.py (Docker build context).
rm -rf "$D/code"
mkdir -p "$D/code"
cp "$PROJECT/constants.py" "$D/code/"
cp -r "$PROJECT/utils" "$D/code/"
cp "$D/inference.py" "$D/code/"
touch "$D/code/utils/__init__.py"   # ensure importable as a package

ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
ECR="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${ECR}/${REPO}:${TAG}"

# 2. Ensure the repo exists.
aws ecr describe-repositories --repository-names "$REPO" --region "$REGION" >/dev/null 2>&1 \
  || aws ecr create-repository --repository-name "$REPO" --region "$REGION" >/dev/null

# 3. Log in to BOTH the DLC source registry (763104351884) and our ECR.
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin 763104351884.dkr.ecr."$REGION".amazonaws.com
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$ECR"

# 4. Build + push.
# Pass the region-matched base image so `docker build` pulls the DLC from the
# same ECR registry we logged into above (the Dockerfile default is us-west-2).
docker build \
  --build-arg BASE_IMAGE="763104351884.dkr.ecr.${REGION}.amazonaws.com/pytorch-inference:2.4.0-gpu-py311-cu124-ubuntu22.04-sagemaker" \
  -t "$IMAGE" "$D"
docker push "$IMAGE"
echo "PUSHED: $IMAGE"
