# Use the latest Ray master as base.
FROM rayproject/ray:nightly-py310
# Invalidate the cache so that fresh code is pulled in the next step.
ARG BUILD_DATE
# Retrieve your development code.
ADD . ray
# Install symlinks to your modified Python code.
RUN python ray/python/ray/setup-dev.py -y
