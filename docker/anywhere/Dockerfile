# The examples Docker image adds dependencies needed to run the examples

#FROM rayproject/ray:latest
FROM rayproject/ray:2.1.0-py38-gpu

ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True

# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN sudo apt-get update && sudo apt-get install -y n2n inadyn && sudo apt-get clean
RUN pip install --no-cache-dir -U pip \
    pandas \
    modin \
    dask[dataframe]==2022.2.0 \
    jupyter
