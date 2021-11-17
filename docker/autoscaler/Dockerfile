ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"

COPY .whl/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl .

RUN $HOME/anaconda3/bin/pip --no-cache-dir install ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl[all]

COPY run_autoscaler.py /home/ray/run_autoscaler.py
COPY run_autoscaler_with_retries.py /home/ray/run_autoscaler_with_retries.py

# Run the autoscaler.
CMD $HOME/anaconda3/bin/python /home/ray/run_autoscaler_with_retries.py