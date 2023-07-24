# Dockerfile used to create the docker image for `03_serving_stable_diffusion`.
FROM anyscale/ray:2.5.1-py39-cu116

COPY requirements.txt ./

RUN pip install --no-cache-dir -U -r requirements.txt

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version