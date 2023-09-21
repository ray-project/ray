ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray:nightly"$BASE_IMAGE"
FROM "$FULL_BASE_IMAGE"

# The python/* paths only exist in civ2, so we put them as non-first arguments. Docker
# will ignore non-existent paths if they are non-first arguments.
#
# TODO(can): simplify this once civ1 is completely deprecated.
COPY *requirements.txt \
     python/*requirements.txt \
     python/requirements/ml/*requirements.txt  \
     python/requirements/docker/*requirements.txt ./
COPY *requirements_compiled.txt \
     python/*requirements_compiled.txt ./
COPY *install-ml-docker-requirements.sh \
     docker/ray-ml/*install-ml-docker-requirements.sh ./

RUN sudo chmod +x install-ml-docker-requirements.sh \
    && ./install-ml-docker-requirements.sh

# Export installed packages
RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt

# Make sure tfp is installed correctly and matches tf version.
RUN python -c "import tensorflow_probability"
