ARG GPU=""
ARG AUTOSCALER="autoscaler"
FROM rayproject/base-deps:latest"$GPU"
ARG WHEEL_PATH
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH .
RUN $HOME/anaconda3/bin/pip --no-cache-dir install $(basename $WHEEL_PATH)[all] \
    $(if [ "$AUTOSCALER" != "autoscaler" ]; then echo \
        "boto3==1.4.8" \
        "google-api-python-client==1.7.8" \
        "azure-cli-core==2.4.0" \
        "azure-mgmt-compute==12.0.0" \
        "azure-mgmt-msi==1.0.0" \
        "azure-mgmt-network==10.1.0"; fi) \
    && $HOME/anaconda3/bin/pip uninstall ray -y && rm $(basename $WHEEL_PATH)
