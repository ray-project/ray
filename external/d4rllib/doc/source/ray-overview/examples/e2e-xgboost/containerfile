# container file used for launching services and jobs.
FROM anyscale/ray:2.44.1-slim-py312-cu123

# Install dependencies.
# Disable cache for a smaller image to optimize build and cluster startup time.
RUN python3 -m pip install --no-cache-dir -r https://raw.githubusercontent.com/anyscale/e2e-xgboost/refs/heads/main/requirements.txt