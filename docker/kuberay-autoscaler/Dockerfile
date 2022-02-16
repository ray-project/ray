FROM rayproject/kuberay-autoscaler
#ARG WHEEL_PATH
#RUN $HOME/anaconda3/bin/pip uninstall -y ray
#COPY $WHEEL_PATH .
#RUN $HOME/anaconda3/bin/pip --no-cache-dir install "$WHEEL_PATH"[all]

COPY run_autoscaler.py /home/ray/run_autoscaler.py
COPY run_autoscaler_with_retries.py /home/ray/run_autoscaler_with_retries.py
COPY autoscaling_config.py /home/ray/autoscaling_config.py
COPY kuberay/ /home/ray/anaconda3/lib/python3.7/site-packages/ray/autoscaler/_private/kuberay/
