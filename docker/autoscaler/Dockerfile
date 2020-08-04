FROM rayproject/ray

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
COPY requirements.txt ./
COPY requirements_autoscaler.txt ./

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements.txt \ 
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements_autoscaler.txt \
    && rm requirements.txt && rm requirements_autoscaler.txt

