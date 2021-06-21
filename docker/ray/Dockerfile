ARG GPU
FROM rayproject/ray-deps:nightly"$GPU"
ARG WHEEL_PATH
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH .
RUN $HOME/anaconda3/bin/pip --no-cache-dir install `basename $WHEEL_PATH`[all] \
&& sudo rm `basename $WHEEL_PATH`
