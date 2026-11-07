# syntax=docker/dockerfile:1
ARG RAY_IMAGE=rayproject/ray:nightly.261107.891a5a-py312@sha256:fe766d202eb5543b6a473ce026efb8001ec2046a33cc8bffe8fe6dbfb1118bbc
FROM ${RAY_IMAGE}

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-kdc krb5-admin-server krb5-user libkrb5-dev gcc libc6-dev openssl \
    && apt-get clean
ARG HIVE_AUTH_REQUIREMENTS=python/deplocks/ci/data-hive-auth-ci_depset_py3.12.lock
COPY ${HIVE_AUTH_REQUIREMENTS} /opt/hive-auth-requirements.txt
RUN python -m venv --system-site-packages /opt/hive-venv \
    && /opt/hive-venv/bin/python -m pip install -r /opt/hive-auth-requirements.txt \
    && chown -R 1000:1000 /opt/hive-venv
ENV PATH=/opt/hive-venv/bin:${PATH}
ENV PYTHONDONTWRITEBYTECODE=1 \
    KRB5_CONFIG=/krb/krb5.conf \
    KRB5_CLIENT_KTNAME=FILE:/nonexistent-client-keytab \
    KRB5CCNAME=FILE:/auth/valid.ccache \
    LC_ALL=C TZ=UTC RAY_USAGE_STATS_ENABLED=0
WORKDIR /ray
