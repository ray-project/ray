FROM rayproject/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME
COPY $WHEEL_PATH $WHEEL_NAME
RUN $HOME/anaconda3/bin/pip --no-cache-dir install $WHEEL_NAME[all]
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8