#!/bin/bash -i
# shellcheck disable=SC2046

set -exuo pipefail

PYTHON="3.9"

build() {
  LIB=$1
  conda create -n rllib_contrib python="$PYTHON" -y
  conda activate rllib_contrib
  (cd rllib_contrib/"$LIB" && pip install -r requirements.txt && pip install -e ".[development]")
  ./ci/env/env_info.sh
  # Download files needed for running the bazel tests.
  wget https://raw.githubusercontent.com/ray-project/ray/releases/2.5.1/rllib/tests/run_regression_tests.py -P rllib_contrib/"$LIB"/
}

test_a2c() {
  build "a2c"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/a2c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/a2c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests,-no_tf_eager_tracing --test_arg=--framework=tf2 rllib_contrib/a2c/...
}

test_alpha_star() {
  build "alpha_star"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/alpha_star/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/alpha_star/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=tf2 rllib_contrib/alpha_star/...
}

test_alpha_zero() {
  build "alpha_zero"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/alpha_zero/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/alpha_zero/...
}

test_apex_ddpg() {
  build "apex_ddpg"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/apex_ddpg/...
}

test_apex_dqn() {
  build "apex_dqn"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/apex_dqn/...
}

test_ars() {
  build "ars"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/ars/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/ars/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=tf2 rllib_contrib/ars/...
}

test_bandit() {
  build "bandit"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/bandit/...
}

test_ddpg() {
  build "ddpg"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/ddpg/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/ddpg/...
}

test_es() {
  build "es"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/es/...
}

test_maddpg() {
  build "maddpg"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/maddpg/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=tf rllib_contrib/maddpg/...
}

test_maml() {
  sudo apt install libosmesa6-dev libgl1-mesa-glx libglfw3 patchelf -y
  mkdir -p /root/.mujoco
  wget https://github.com/google-deepmind/mujoco/releases/download/2.1.1/mujoco-2.1.1-linux-x86_64.tar.gz
  mv mujoco-2.1.1-linux-x86_64.tar.gz /root/.mujoco/.
  (cd /root/.mujoco && tar -xf /root/.mujoco/mujoco-2.1.1-linux-x86_64.tar.gz)
  export LD_LIBRARY_PATH=/root/.mujoco/mujoco-2.1.1/bin
  build "maml"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/maml/...
}

test_pg() {
  build "pg"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/pg/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/pg/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests,-no_tf_eager_tracing --test_arg=--framework=tf2 rllib_contrib/pg/...
}

test_qmix() {
  build "qmix"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/qmix/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/qmix/...
}

test_r2d2() {
  build "r2d2"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/r2d2/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/r2d2/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests,-no_tf_eager_tracing --test_arg=--framework=tf2 rllib_contrib/r2d2/...
}

test_simple_q() {
  build "simple_q"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/simple_q/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/simple_q/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests,-no_tf_eager_tracing --test_arg=--framework=tf2 rllib_contrib/simple_q/...
}

test_slate_q() {
  build "slate_q"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/slate_q/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/slate_q/...
}

test_td3() {
  build "td3"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/td3/...
}

test_a3c() {
  build "a3c"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/a3c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/a3c/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=tf2 rllib_contrib/a3c/...
}

test_crr() {
  build "crr"
  wget https://raw.githubusercontent.com/ray-project/ray/master/rllib/tests/data/pendulum/pendulum_replay_v1.1.0.zip -P rllib_contrib/crr/tuned_examples/
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/crr/...
}

test_ddppo() {
  build "ddppo"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/ddppo/...
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,learning_tests --test_arg=--framework=torch rllib_contrib/ddppo/...
}

test_dt() {
  build "dt"
  wget https://github.com/ray-project/ray/raw/releases/2.5.1/rllib/tests/data/pendulum/pendulum_expert_sac_50eps.zip -P rllib_contrib/dt/tuned_examples/
  wget https://github.com/ray-project/ray/raw/releases/2.5.1/rllib/tests/data/pendulum/pendulum_medium_sac_50eps.zip -P rllib_contrib/dt/tuned_examples/
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky,-learning_tests rllib_contrib/dt/...
}

test_leela_chess_zero() {
  build "leela_chess_zero"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/leela_chess_zero/...
}

test_mbmpo() {
  # Install mujoco necessary for the testing environments
  sudo apt install libosmesa6-dev libgl1-mesa-glx libglfw3 patchelf -y
  mkdir -p /root/.mujoco
  wget https://github.com/google-deepmind/mujoco/releases/download/2.1.1/mujoco-2.1.1-linux-x86_64.tar.gz
  mv mujoco-2.1.1-linux-x86_64.tar.gz /root/.mujoco/.
  (cd /root/.mujoco && tar -xf /root/.mujoco/mujoco-2.1.1-linux-x86_64.tar.gz)
  # shellcheck disable=SC2016
  echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/root/.mujoco/mujoco-2.1.1/bin' >> /root/.bashrc
  source /root/.bashrc
  # build
  build "mbmpo"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/mbmpo/...
}

test_td3() {
  build "td3"
  # BAZEL (learning and compilation) tests:
  bazel test --config=ci $(./ci/run/bazel_export_options) --build_tests_only --test_tag_filters=-flaky rllib_contrib/td3/...
}

"$@"
