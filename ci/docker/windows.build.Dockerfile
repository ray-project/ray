FROM rayproject/buildenv:windows

COPY . .
RUN bash ci/ray_ci/windows/build_base.sh
