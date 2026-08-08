# Custom SageMaker inference image = PyTorch inference DLC + ffmpeg + our code.
# A custom image is required because ffmpeg is a system (apt) dependency the
# stock DLC lacks; the DLC brings the sagemaker-inference serving stack.
#
# BASE_IMAGE: pick the current PyTorch *inference* GPU DLC for your region from
# https://github.com/aws/deep-learning-containers/blob/master/available_images.md
# (account 763104351884 is the AWS DLC registry for us-west-2).
ARG BASE_IMAGE=763104351884.dkr.ecr.us-west-2.amazonaws.com/pytorch-inference:2.4.0-gpu-py311-cu124-ubuntu22.04-sagemaker
FROM ${BASE_IMAGE}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# App code (constants.py + utils/ + inference.py), staged into code/ by
# build_and_push.sh. The inference toolkit imports the handler from here.
COPY code/ /opt/ml/code/
ENV SAGEMAKER_SUBMIT_DIRECTORY=/opt/ml/code
ENV SAGEMAKER_PROGRAM=inference.py
