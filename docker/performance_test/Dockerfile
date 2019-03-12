FROM python:3.6.6-stretch

RUN pip install -q awscli pytest-benchmark

COPY . .
WORKDIR ./python
RUN pip install -e .

ENTRYPOINT ["bash", "ci/jenkins_tests/run_perf_test.sh"]