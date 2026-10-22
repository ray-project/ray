# syntax=docker/dockerfile:1
ARG RAY_IMAGE=rayproject/ray:2.59.0-py312
FROM ${RAY_IMAGE}

USER root
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-kdc krb5-admin-server krb5-user libkrb5-dev gcc libc6-dev openssl \
    && apt-get clean
ARG HIVE_AUTH_REQUIREMENTS=python/requirements/data/hive-auth.txt
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
USER 1000
WORKDIR /ray
