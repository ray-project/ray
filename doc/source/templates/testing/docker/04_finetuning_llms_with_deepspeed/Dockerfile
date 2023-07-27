# Dockerfile used to create the docker image for `03_serving_stable_diffusion`.
FROM anyscale/ray:2.6.1-py39-cu117

COPY requirements.txt ./

RUN sudo apt-get update
RUN sudo apt-get install -y libaio1

RUN pip install -U -r requirements.txt

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version