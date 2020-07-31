FROM rayproject/base-deps
ARG WHEEL_PATH
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH ray_wheel.whl
RUN $HOME/anaconda3/bin/pip --no-cache-dir install ray_wheel.whl[all]
