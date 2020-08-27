ARG GPU
FROM rayproject/ray:latest"$GPU"

# We have to uninstall wrapt this way for Tensorflow compatibility
COPY requirements.txt ./
COPY requirements_autoscaler.txt ./

RUN apt-get update \
    && apt-get install -y gcc cmake \ 
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements.txt \ 
    && $HOME/anaconda3/bin/pip --no-cache-dir install -r requirements_autoscaler.txt \
    && rm requirements.txt && rm requirements_autoscaler.txt \
    && apt-get remove cmake gcc -y \
    && apt-get clean 

