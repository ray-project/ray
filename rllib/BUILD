py_test(
    name = "test_dqn_cartpole_v0_softq",
    main = "train.py", srcs = ["train.py"],
    data = glob([":tests/data/cartpole_small/**"]),
    args = [
        "--env", "CartPole-v0",
        "--run", "DQN",
        "--stop", "'{\"training_iteration\": 1}'",
        "--config", "'{\"input\": \"tests/data/cartpole_small\", \"learning_starts\": 0, \"input_evaluation\": [\"wis\", \"is\"], \"soft_q\": true}'"
        ]
)
