# Dockerfile used to create the docker image for `04_finetuning_llms_with_deepspeed`.
FROM anyscale/ray:2.9.0-py310-cu121

COPY requirements.txt ./

RUN sudo apt-get update
RUN sudo apt-get install -y libaio1

RUN pip install --upgrade pip
# We need pydantic at this version to install deepspeed 0.10.2 (as part of the requirements.txt)
RUN pip install pydantic==1.10.7
RUN pip install -U -r requirements.txt
RUN pip install torch==2.1.1 --index-url https://download.pytorch.org/whl/cu121
RUN pip install flash-attn==2.4.2 --no-build-isolation

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version