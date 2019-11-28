FROM ray-project/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME

RUN apt update
RUN apt install -y curl tmux screen rsync

# We have to uninstall wrapt this way for Tensorflow compatibility
RUN conda remove -y wrapt
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY $WHEEL_PATH $WHEEL_NAME
RUN pip install $WHEEL_NAME[all]

# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
