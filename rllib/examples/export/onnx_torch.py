from packaging.version import Version
import numpy as np
import ray
import ray.rllib.algorithms.ppo as ppo
import onnxruntime
import os
import shutil
import torch

from ray.rllib.utils.torch_utils import convert_to_torch_tensor


class ONNXCompatibleWrapper(torch.nn.Module):
    def __init__(self, original_model):
        super(ONNXCompatibleWrapper, self).__init__()
        self.original_model = original_model

    def forward(self, a, b0, b1, c):
        # Convert the separate tensor inputs back into the list format
        # expected by the original model's forward method.
        b = [b0, b1]
        ret = self.original_model({"obs": a}, b, c)
        # results, state_out_0, state_out_1
        return ret[0], ret[1][0], ret[1][1]


if __name__ == "__main__":
    # Configure our PPO Algorithm.
    config = (
        ppo.PPOConfig()
        # ONNX is not supported by RLModule API yet.
        .experimental(_enable_new_api_stack=False)
        .environment("CartPole-v1")
        .rollouts(num_rollout_workers=1)
        .framework("torch")
        .training(model={"use_lstm": True})
    )

    B = 3
    T = 5
    LSTM_CELL = 256

    # Input data for a python inference forward call.
    test_data_python = {
        "obs": np.random.uniform(0, 1.0, size=(B*T, 4)).astype(np.float32),
        "state_ins": [
            np.random.uniform(0, 1.0, size=(B, LSTM_CELL)).astype(np.float32),
            np.random.uniform(0, 1.0, size=(B, LSTM_CELL)).astype(np.float32)
        ],
        "seq_lens": np.array([T] * B, np.float32),
    }
    # Input data for the ONNX session.
    test_data_onnx = {
        "obs": test_data_python["obs"],
        "state_in_0": test_data_python["state_ins"][0],
        "state_in_1": test_data_python["state_ins"][1],
        "seq_lens": test_data_python["seq_lens"],
    }
    # Input data for compiling the ONNX model.
    test_data_onnx_input = convert_to_torch_tensor(test_data_onnx)

    # Start Ray and initialize a PPO Algorithm.
    ray.init()
    algo = config.build()

    # Let's run inference on the torch model
    policy = algo.get_policy()
    result_pytorch, _ = policy.model(
        {
            "obs": torch.tensor(test_data_python["obs"]),
        },
        [
            torch.tensor(test_data_python["state_ins"][0]),
            torch.tensor(test_data_python["state_ins"][1]),
        ],
        torch.tensor(test_data_python["seq_lens"]),
    )

    # Evaluate tensor to fetch numpy array
    result_pytorch = result_pytorch.detach().numpy()

    # This line will export the model to ONNX.
    onnx_compatible = ONNXCompatibleWrapper(policy.model)
    exported_model_file = "model.onnx"
    torch.onnx.export(
        onnx_compatible,
        (test_data_onnx_input["obs"], test_data_onnx_input["state_in_0"], test_data_onnx_input["state_in_1"], test_data_onnx_input["seq_lens"]),
        exported_model_file,
        export_params=True,
        opset_version=11,
        do_constant_folding=True,
        input_names=["obs", "state_in_0", "state_in_1", "seq_lens"],
        output_names=["output", "state_out_0", "state_out_1"],
        dynamic_axes={
            k: {0: "batch_size"}
            for k in ["obs", "state_in_0", "state_in_1", "seq_lens"]
        },
    )
    # Start an inference session for the ONNX model
    session = onnxruntime.InferenceSession(exported_model_file, None)
    result_onnx = session.run(["output"], test_data_onnx)

    # These results should be equal!
    print("PYTORCH", result_pytorch)
    print("ONNX", result_onnx[0])

    assert np.allclose(
        result_pytorch, result_onnx
    ), "Model outputs are NOT equal. FAILED"
    print("Model outputs are equal. PASSED")
