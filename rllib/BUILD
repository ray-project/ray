# --------------------------------------------------------------------
# BAZEL/Travis-ci test cases.
#
# NOTE: Move more test cases here from ci/jenkins/run_rllib_tests.sh
# as Travis tests seem to run more stable.
# --------------------------------------------------------------------

#load("//bazel:rllib_train.bzl", "train")

# ---------------------------------------
# Agents (short learning tasks)
# ---------------------------------------

# TODO:

# ---------------------------------------
# Agents (Compilation and Losses)
# ---------------------------------------

# PGTrainer
py_test(
    name = "test_pg",
    size = "small",
    srcs = ["agents/pg/tests/test_pg.py"]
)

# PPOTrainer
py_test(
    name = "test_ppo",
    size = "small",
    srcs = ["agents/ppo/tests/test_ppo.py",
            "agents/ppo/tests/test.py"]  # TODO: Move down once PR 6889 merged
)

# ---------------------------------------
# Agents (quick training test iterations)
# ---------------------------------------

py_binary(
    name = "test_a3c_pong_deterministic_v0",
    srcs = "rllib/train.py",
    args = [
        "--env", "PongDeterministic-v0",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'"
        ]
)

py_binary(
    name = "test_a3c_pong_ram_v4",
    srcs = "rllib/train.py",
    args = [
        "--env", "Pong-ram-v4",
        "--run", "A3C",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"num_workers\": 2}'"
        ]
)



# ---------------------------------------
# Models and Distributions
# ---------------------------------------

# TODO: Move here once PR 6889 merged
#py_test(
#    name = "test_distributions",
#    size = "small",
#    srcs = ["models/tests/test_distributions.py"]
#)

# ---------------------------------------
# Utils
# ---------------------------------------

py_test(
    name = "test_framework_agnostic_components",
    size = "small",
    srcs = ["utils/tests/test_framework_agnostic_components.py"]
)


