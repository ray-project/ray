FROM rayproject/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME

# Install kubectl.
RUN apt-get update \
    && apt-get install -y apt-transport-https \
    && curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && touch /etc/apt/sources.list.d/kubernetes.list \
    && echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list \
    && apt-get update \
    && apt install -y kubectl \
    && apt-get clean 

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt .
COPY $WHEEL_PATH $WHEEL_NAME

RUN pip --no-cache-dir install -r requirements.txt \
    && pip --no-cache-dir install $WHEEL_NAME[all]

# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
