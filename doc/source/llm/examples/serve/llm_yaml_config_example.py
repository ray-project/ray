"""
This file serves as a CI test for YAML config deployment.

Structure:
1. Create modified config without accelerator requirements for CI testing
2. Start `serve run config.yaml` as subprocess
3. Test validation (deployment status polling + cleanup)
"""

import time
import os
import subprocess
import tempfile
import yaml
from ray import serve
from ray.serve.schema import ApplicationStatus

config_path = os.path.join(os.path.dirname(__file__), "llm_config_example.yaml")

# Load YAML config and remove accelerator_type for CI testing
with open(config_path, "r") as f:
    config_dict = yaml.safe_load(f)

for llm_config in config_dict["applications"][0]["args"]["llm_configs"]:
    llm_config.pop("accelerator_type", None)

# Write modified config to temporary file
with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
    yaml.dump(config_dict, f)
    temp_config_path = f.name

# Start serve run subprocess
process = subprocess.Popen(
    ["serve", "run", temp_config_path, "--non-blocking"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
)

# Wait for deployment to reach RUNNING status
app_name = "llm_app"
status = ApplicationStatus.NOT_STARTED
start_time = time.time()
timeout_seconds = 300

while status != ApplicationStatus.RUNNING and time.time() - start_time < timeout_seconds:
    # Check if subprocess failed early
    if process.poll() is not None and process.returncode != 0:
        stdout, stderr = process.communicate()
        print(f"Subprocess failed:\n{stdout}\n{stderr}")
        raise AssertionError(f"Serve subprocess failed with exit code: {process.returncode}")
    
    # Print subprocess output when it completes
    if process.poll() is not None and process.returncode == 0:
        stdout, stderr = process.communicate()
        if stdout:
            print(f"Serve output:\n{stdout}")
    
    # Check deployment status
    serve_status = serve.status()
    if app_name in serve_status.applications:
        new_status = serve_status.applications[app_name].status
        if new_status != status:
            print(f"Status: {new_status}")
            status = new_status
        
        if status in [ApplicationStatus.DEPLOY_FAILED, ApplicationStatus.UNHEALTHY]:
            raise AssertionError(f"Deployment failed with status: {status}")

    time.sleep(2)

if status != ApplicationStatus.RUNNING:
    raise AssertionError(f"Deployment timed out. Current status: {status}")


process.terminate()
serve.shutdown()
os.remove(temp_config_path)
