FROM ray-project/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME

# We have to uninstall wrapt this way for Tensorflow compatibility
RUN conda remove -y wrapt
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY $WHEEL_PATH $WHEEL_NAME
RUN pip install $WHEEL_NAME[all]

