FROM ray-project/base-deps
ARG WHEEL_PATH
ARG WHEEL_NAME

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY $WHEEL_PATH $WHEEL_NAME
RUN pip install $WHEEL_NAME

