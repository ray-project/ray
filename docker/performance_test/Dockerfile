FROM python:3.6.6-stretch

FROM ray-project/base-deps
ENV RAY_USE_CMAKE=1
ADD ray.tar /ray
ADD git-rev /ray/git-rev
WORKDIR /ray/python
RUN pip install -e .
WORKDIR /ray
RUN pip install -q awscli pytest-benchmark

ENTRYPOINT ["bash", "ci/jenkins_tests/perf_integration_tests/run_perf_integration.sh"]
