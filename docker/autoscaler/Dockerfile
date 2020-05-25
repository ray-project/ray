FROM rayproject/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME

RUN apt update
RUN apt install -y curl tmux screen rsync apt-transport-https

# Install kubectl.
RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN touch /etc/apt/sources.list.d/kubernetes.list
RUN echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list
RUN apt update
RUN apt install -y kubectl

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY $WHEEL_PATH $WHEEL_NAME
RUN pip install $WHEEL_NAME[all]

# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
