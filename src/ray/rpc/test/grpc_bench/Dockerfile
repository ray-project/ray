FROM ubuntu:22.04
RUN apt-get update -y && apt-get install -y libjemalloc-dev

COPY grpc_bench /

ENTRYPOINT LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 /grpc_bench

