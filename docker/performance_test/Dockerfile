FROM python:3.6.6-stretch

RUN pip install -q awscli pytest-benchmark
RUN bash ci/travis/install-bazel.sh

COPY . .
RUN pip install -e .

ENTRYPOINT ["bash", "ci/jenkins_tests/run_perf_integration.sh"]
