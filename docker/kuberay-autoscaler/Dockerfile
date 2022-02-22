FROM rayproject/ray:nightly
ARG WHEEL_PATH
RUN $HOME/anaconda3/bin/pip uninstall -y ray
COPY $WHEEL_PATH .
RUN $HOME/anaconda3/bin/pip --no-cache-dir install "$WHEEL_PATH"[all]

COPY run_autoscaler.py /home/ray/run_autoscaler.py
COPY run_autoscaler_with_retries.py /home/ray/run_autoscaler_with_retries.py
