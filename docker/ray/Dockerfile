ARG GPU
FROM rayproject/ray-deps:nightly"$GPU"
ARG WHEEL_PATH
ARG RAY_UID=1000
ARG RAY_GID=100

# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
COPY --chown=$RAY_UID:$RAY_GID $WHEEL_PATH .
# Create a user with $RAY_UID if that user does not already exist.
RUN getent passwd $RAY_UID || useradd --no-create-home --no-user-group ray --uid $RAY_UID --gid $RAY_GID
USER $RAY_UID
RUN stat `basename $WHEEL_PATH`
RUN $HOME/anaconda3/bin/pip --no-cache-dir install `basename $WHEEL_PATH`[all] \
&& rm `basename $WHEEL_PATH`
