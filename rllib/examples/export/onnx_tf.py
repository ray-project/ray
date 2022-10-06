import argparse
import numpy as np
import onnxruntime
import os
import shutil

import ray
import ray.rllib.algorithms.ppo as ppo

parser = argparse.ArgumentParser()

parser.add_argument(
    "--framework",
    choices=["tf", "tf2"],
    default="tf",
    help="The TF framework specifier (either 'tf' or 'tf2').",
)


if __name__ == "__main__":

    args = parser.parse_args()

    # Configure our PPO trainer
    config = ppo.PPOConfig().rollouts(num_rollout_workers=1).framework(args.framework)

    outdir = "export_tf"
    if os.path.exists(outdir):
        shutil.rmtree(outdir)

    np.random.seed(1234)

    # We will run inference with this test batch
    test_data = {
        "obs": np.random.uniform(0, 1.0, size=(10, 4)).astype(np.float32),
    }

    # Start Ray and initialize a PPO Algorithm
    ray.init()
    algo = config.build(env="CartPole-v0")

    # You could train the model here via:
    # algo.train()

    # Let's run inference on the tensorflow model
    policy = algo.get_policy()
    result_tf, _ = policy.model(test_data)

    # Evaluate tensor to fetch numpy array.
    if args.framework == "tf":
        with policy.get_session().as_default():
            result_tf = result_tf.eval()

    # This line will export the model to ONNX.
    policy.export_model(outdir, onnx=11)
    # Equivalent to:
    # algo.export_policy_model(outdir, onnx=11)

    # Import ONNX model.
    exported_model_file = os.path.join(outdir, "model.onnx")

    # Start an inference session for the ONNX model
    session = onnxruntime.InferenceSession(exported_model_file, None)

    # Pass the same test batch to the ONNX model (rename to match tensor names)
    onnx_test_data = {f"default_policy/{k}:0": v for k, v in test_data.items()}

    # Tf2 model stored differently from tf (static graph) model.
    if args.framework == "tf2":
        result_onnx = session.run(["fc_out"], {"observations": test_data["obs"]})
    else:
        result_onnx = session.run(
            ["default_policy/model/fc_out/BiasAdd:0"],
            onnx_test_data,
        )

    # These results should be equal!
    print("TENSORFLOW", result_tf)
    print("ONNX", result_onnx)

    assert np.allclose(result_tf, result_onnx), "Model outputs are NOT equal. FAILED"
    print("Model outputs are equal. PASSED")
