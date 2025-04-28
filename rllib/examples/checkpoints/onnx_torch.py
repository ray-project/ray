# @OldAPIStack

from packaging.version import Version
import numpy as np
import ray
import ray.rllib.algorithms.ppo as ppo
import onnxruntime
import os
import shutil
import torch

if __name__ == "__main__":
    # Configure our PPO Algorithm.
    config = (
        ppo.PPOConfig()
        .api_stack(
            enable_env_runner_and_connector_v2=False,
            enable_rl_module_and_learner=False,
        )
        .env_runners(num_env_runners=1)
        .framework("torch")
    )

    outdir = "export_torch"
    if os.path.exists(outdir):
        shutil.rmtree(outdir)

    np.random.seed(1234)

    # We will run inference with this test batch
    test_data = {
        "obs": np.random.uniform(0, 1.0, size=(10, 4)).astype(np.float32),
        "state_ins": np.array([0.0], dtype=np.float32),
    }

    # Start Ray and initialize a PPO Algorithm.
    ray.init()
    algo = config.build(env="CartPole-v1")

    # You could train the model here
    # algo.train()

    # Let's run inference on the torch model
    policy = algo.get_policy()
    result_pytorch, _ = policy.model(
        {
            "obs": torch.tensor(test_data["obs"]),
        }
    )

    # Evaluate tensor to fetch numpy array
    result_pytorch = result_pytorch.detach().numpy()

    # This line will export the model to ONNX.
    policy.export_model(outdir, onnx=11)
    # Equivalent to:
    # algo.export_policy_model(outdir, onnx=11)

    # Import ONNX model.
    exported_model_file = os.path.join(outdir, "model.onnx")

    # Start an inference session for the ONNX model
    session = onnxruntime.InferenceSession(exported_model_file, None)

    # Pass the same test batch to the ONNX model
    if Version(torch.__version__) < Version("1.9.0"):
        # In torch < 1.9.0 the second input/output name gets mixed up
        test_data["state_outs"] = test_data.pop("state_ins")

    result_onnx = session.run(["output"], test_data)

    # These results should be equal!
    print("PYTORCH", result_pytorch)
    print("ONNX", result_onnx)

    assert np.allclose(
        result_pytorch, result_onnx
    ), "Model outputs are NOT equal. FAILED"
    print("Model outputs are equal. PASSED")
