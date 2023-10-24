# Dockerfile used to create the docker image for `04_finetuning_llms_with_deepspeed`.
FROM anyscale/ray:2.7.1-py310-cu121

COPY requirements.txt ./

RUN sudo apt-get update
RUN sudo apt-get install -y libaio1

RUN pip install -U -r requirements.txt

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version