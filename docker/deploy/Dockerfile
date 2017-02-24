# The deploy Docker image build a self-contained Ray instance suitable
# for end users.

FROM ray-project/base-deps
ADD ray.tar /ray
ADD git-rev /ray/git-rev
WORKDIR /ray/python
RUN python setup.py install
WORKDIR /ray
RUN echo "tail -f /dev/null" >> scripts/start_ray.sh
