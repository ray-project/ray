ARG BASE_IMAGE=""
FROM rayproject/base-deps:nightly"$BASE_IMAGE"
ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH
RUN $HOME/anaconda3/bin/pip --no-cache-dir install --find-links $FIND_LINKS_PATH \
    $(basename $WHEEL_PATH)[all] \
    && $HOME/anaconda3/bin/pip uninstall ray -y && sudo rm $(basename $WHEEL_PATH)
