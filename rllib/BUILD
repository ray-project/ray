# --------------------------------------------------------------------
# BAZEL/Travis-ci test cases.
#
# NOTE: Move more test cases here from ci/jenkins/run_rllib_tests.sh
# as Travis tests seem to run more stable.
# --------------------------------------------------------------------


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
# Models and Distributions
# ---------------------------------------

# TODO: Move here once PR 6889 merged
#py_test(
#    name = "test_distributions",
#    size = "small",
#    srcs = ["models/tests/test_distributions.py"]
#)

# Schedules
py_test(
    name = "test_schedules",
    size = "small",
    srcs = ["utils/schedules/tests/test_schedules.py"]
)
