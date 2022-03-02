ARG BASE_IMAGE=""
FROM rayproject/base-deps:nightly"$BASE_IMAGE"
# If this arg is not "autoscaler" then no autoscaler requirements will be included
ARG AUTOSCALER="autoscaler"
ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH
RUN $HOME/anaconda3/bin/pip --no-cache-dir install --find-links $FIND_LINKS_PATH \
    $(basename $WHEEL_PATH)[all] \
    $(if [ "$AUTOSCALER" = "autoscaler" ]; then echo \
        "six==1.13.0" \
        "boto3==1.4.8" \
        "google-api-python-client==1.7.8" \
        "google-oauth" \
        "kubernetes" \
        "azure-cli-core==2.29.1" \
        "azure-identity==1.7.0" \
        "azure-mgmt-compute==23.1.0" \
        "azure-mgmt-network==19.0.0" \
        "azure-mgmt-resource==20.0.0" \
        "msrestazure==0.6.4"; fi) \
    $(if [ $($HOME/anaconda3/bin/python -c "import sys; print(sys.version_info.minor)") != 6 ] \
        && [ "$AUTOSCALER" = "autoscaler" ]; then echo "kopf"; fi) \
    && $HOME/anaconda3/bin/pip uninstall ray -y && sudo rm $(basename $WHEEL_PATH)
