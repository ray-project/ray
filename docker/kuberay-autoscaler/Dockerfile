FROM rayproject/ray:nightly

COPY autoscaling_config.py /home/ray/autoscaling_config.py
COPY run_autoscaler.py /home/ray/run_autoscaler.py
COPY run_autoscaler_with_retries.py /home/ray/run_autoscaler_with_retries.py
RUN mkdir -p /tmp/ray/session_latest/logs
