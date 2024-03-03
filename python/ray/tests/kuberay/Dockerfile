# Use the latest Ray master as base.
# FROM rayproject/ray:nightly-py38
# FROM 029272617770.dkr.ecr.us-west-2.amazonaws.com/rayproject/citemp:fda7d5b7-minbuild-core-py3.8@sha256:604a746d88351e4f10a813ba4a1298be606c6e9f5c2bf444ae1322678276fc38
FROM rayproject/ray:827d5e-py39-cpu
# Invalidate the cache so that fresh code is pulled in the next step.
ARG BUILD_DATE
# Retrieve your development code.
ADD . ray
# Install symlinks to your modified Python code.
# RUN pip install -e ray/python/ -v
RUN python ray/python/ray/setup-dev.py -y
