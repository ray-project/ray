FROM ubuntu:latest

# If you wish to default to bash shell instead of sh(Bourne) shell:
#     RUN rm /bin/sh && ln -s /bin/bash /bin/sh

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# disable interactive functions, suppress tzdata prompting for geographic location
ENV DEBIAN_FRONTEND noninteractive

RUN apt -qq update \
    && apt-get -qq install --no-install-recommends apt-utils \
    && apt-get -qq install git curl nano openssh-server \
    && apt-get -qq install python-opencv

# Install miniconda3
RUN curl -LO https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh
RUN bash Miniconda3-4.5.4-Linux-x86_64.sh -p /miniconda3 -b
RUN rm Miniconda3-4.5.4-Linux-x86_64.sh
ENV PATH=/miniconda3/bin:${PATH}
RUN conda update -y conda
#don't use python3.7, have to use wheel's url to pip install some pkgs due to name conflicts



#update pip to 18.1 from conda-forge (or replace this section with: `pip install --upgrade pip`).
#`pip install --upgrade` might be faster
RUN conda config --add channels conda-forge && \
    conda update pip
#conda install libgcc needed for `pip install lz4`
#libgcc will be installed if you create env with `conda create`


#Install ray and relevant python packages
RUN pip install ray pssh tensorflow gym scipy opencv-python bokeh jupyter


#Conda4.4 or later requires the following for `conda activate` to work
RUN echo ". /miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc
RUN ln -s /miniconda3/etc/profile.d/conda.sh /etc/profile.d/conda.sh

#Generate SSH key-pair
#This creates the SSH key once when you build the Docker Image
#All Kubernetes pods from same image will have same SSH key-pairs
#If you want different SSH keys for each pod, do this in `head.yml` and `worker.yml` instead
RUN ssh-keygen -f /root/.ssh/id_rsa -P "" \
    && echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config \
    && touch ~/.ssh/authorized_keys \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
