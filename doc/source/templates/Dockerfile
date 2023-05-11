# Use Anyscale base image
FROM anyscale/ray:latest-py39-cu118

COPY requirements.txt ./

RUN pip install --no-cache-dir -U -r requirements.txt

RUN pip install --no-cache-dir ipywidgets

RUN echo "Testing Ray Import..." && python -c "import ray"
RUN ray --version
