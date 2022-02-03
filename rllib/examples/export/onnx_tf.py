import numpy as np
import ray
import ray.rllib.agents.ppo as ppo
import onnxruntime
import os
import shutil

# Configure our PPO trainer
config = ppo.DEFAULT_CONFIG.copy()
config["num_gpus"] = 0
config["num_workers"] = 1
config["framework"] = "tf"

outdir = "export_tf"
if os.path.exists(outdir):
    shutil.rmtree(outdir)

np.random.seed(1234)

# We will run inference with this test batch
test_data = {
    "obs": np.random.uniform(0, 1.0, size=(10, 4)).astype(np.float32),
}

# Start Ray and initialize a PPO trainer
ray.init()
trainer = ppo.PPOTrainer(config=config, env="CartPole-v0")

# You could train the model here
# trainer.train()

# Let's run inference on the tensorflow model
policy = trainer.get_policy()
result_tf, _ = policy.model(test_data)

# Evaluate tensor to fetch numpy array
with policy._sess.as_default():
    result_tf = result_tf.eval()

# This line will export the model to ONNX
res = trainer.export_policy_model(outdir, onnx=11)

# Import ONNX model
exported_model_file = os.path.join(outdir, "saved_model.onnx")

# Start an inference session for the ONNX model
session = onnxruntime.InferenceSession(exported_model_file, None)

# Pass the same test batch to the ONNX model (rename to match tensor names)
onnx_test_data = {f"default_policy/{k}:0": v for k, v in test_data.items()}

result_onnx = session.run(["default_policy/model/fc_out/BiasAdd:0"], onnx_test_data)

# These results should be equal!
print("TENSORFLOW", result_tf)
print("ONNX", result_onnx)

assert np.allclose(result_tf, result_onnx), "Model outputs are NOT equal. FAILED"
print("Model outputs are equal. PASSED")
