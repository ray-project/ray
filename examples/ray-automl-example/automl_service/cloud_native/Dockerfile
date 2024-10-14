FROM ubuntu:23.04
RUN apt update
RUN apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev curl software-properties-common wget libbz2-dev inetutils-ping -y
RUN wget https://www.python.org/ftp/python/3.9.16/Python-3.9.16.tgz && tar xvf Python-*.tgz && cd Python-3.9*/ && ./configure --enable-optimizations && make altinstall
RUN ln -s /usr/local/bin/python3.9 /usr/bin/python -f
COPY python/ /python
WORKDIR /python
RUN python -m pip install -r /python/requirements.txt
