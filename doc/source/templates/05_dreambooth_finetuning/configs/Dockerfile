# Run `docker build` with this from the 05_dreambooth_finetuning directory
FROM anyscale/ray:latest-py39-cu118

COPY dreambooth/requirements.txt ./

RUN pip install --no-cache-dir -U -r requirements.txt

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version
