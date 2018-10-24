FROM ubuntu:latest

# If you wish to default to bash shell instead of sh(Bourne) shell:
#     RUN rm /bin/sh && ln -s /bin/bash /bin/sh

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# disable interactive functions, suppress tzdata prompting for geographic location
ENV DEBIAN_FRONTEND noninteractive

RUN apt -qq update \
    && apt-get -qq install --no-install-recommends apt-utils \
    && apt-get -qq install git wget \
    && apt-get -qq install cmake pkg-config build-essential autoconf curl libtool unzip flex bison psmisc nano openssh-server \
    && apt-get -qq install python-opencv

# Install miniconda3, create env named: ray-kubernetes
RUN curl -LO http://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN bash Miniconda3-latest-Linux-x86_64.sh -p /miniconda3 -b
RUN rm Miniconda3-latest-Linux-x86_64.sh
ENV PATH=/miniconda3/bin:${PATH}
RUN conda update -y conda
RUN conda create -y -n ray-kubernetes python=3.6.5
#don't use 3.7, have to use wheel's url to pip install some pkgs due to name conflicts



#Set ray-kubernetes as current environment
#if you instead alter then run ~/.bashrc, and use conda activate: Everything must be within the same Docker RUN command
ENV PATH /miniconda3/envs/ray-kubernetes/bin:$PATH


#update pip to 18.1 from conda-forge (or replace this section with: `pip install --upgrade pip`).
#`pip install --upgrade` might be faster
RUN conda config --add channels conda-forge && \
    conda update -n ray-kubernetes pip && \
    conda install libgcc
#libgcc might be already downloaded with conda create


#Install ray and relevant python packages
RUN pip install ray pssh cython pyarrow tensorflow gym scipy opencv-python bokeh jupyter lz4


#Conda4.4 or later requires the following
RUN echo ". /miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc
RUN ln -s /miniconda3/etc/profile.d/conda.sh /etc/profile.d/conda.sh

#activate ray-kubernetes ENV by default
#RUN echo conda activate ray-kubernetes >> ~/.bashrc

#Generate SSH key-pair
#This creates the SSH key once when you build the Docker Image
#All Kubernetes pods from same image will have same SSH key-pairs
#If you want different SSH keys for each pod, do this in `head.yml` and `worker.yml` instead
RUN ssh-keygen -f /root/.ssh/id_rsa -P "" \
    && echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

#Clone the ray-kubes git repo for test codes
RUN git clone https://github.com/jhpenger/ray-kubernetes-1.git
