ARG BASE_IMAGE
FROM rayproject/ray:nightly"$BASE_IMAGE"

ENV PATH /root/anaconda3/bin:$PATH

COPY run_autoscaler.py /home/ray/run_autoscaler.py
COPY run_autoscaler_with_retries.py /home/ray/run_autoscaler_with_retries.py

# Run the autoscaler.
CMD python /home/ray/run_autoscaler_with_retries.py