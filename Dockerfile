# Use an official Python image as the base
FROM python:3.9-slim

# Install dependencies
RUN apt update && apt install -y \
    curl \
    gnupg \ 
    git \
    build-essential

# Copy the GPG file into the container
COPY bazel-release.pub.gpg /tmp/bazel-release.pub.gpg
# Add the GPG key
RUN gpg --dearmor < /tmp/bazel-release.pub.gpg > /usr/share/keyrings/bazel-archive-keyring.gpg

# Add the Bazel repository
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

# Install Bazel
RUN apt update && apt install bazel-6.5.0 -y
RUN apt update && apt full-upgrade -y
RUN find / -iname "bazel-6.5.0" 2>/dev/null #&& which bazel-6.5.0
RUN ln -s /usr/bin/bazel-6.5.0 /usr/bin/bazel #&& which bazel
RUN pip install psutil setproctitle==1.2.2 colorama
COPY . ./ray 
WORKDIR /ray
RUN bash ./build.sh
