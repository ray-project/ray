from enum import Enum

# Mapping of component -> partial log file name.
RAY_LOG_CATEGORIES = {
    "worker": "worker",
    "core_worker": "core_worker",
    "driver": "driver",
    "runtime_env": "runtime_env",
    "dashboard": "dashboard",
    "ray_client",
}
            if "worker" in log_file and (log_file.endswith(".out") or log_file.endswith(".err")):
                result["worker"].append(log_file)
            elif "core-worker" in log_file and log_file.endswith(".log"):
                result["core_worker"].append(log_file)
            elif "core-driver" in log_file and log_file.endswith(".log"):
                result["driver"].append(log_file)
            elif "raylet." in log_file:
                result["raylet"].append(log_file)
            elif "gcs_server." in log_file:
                result["gcs_server"].append(log_file)
            elif "log_monitor" in log_file:
                result["internal"].append(log_file)
            elif "monitor" in log_file:
                result["autoscaler"].append(log_file)
            elif "agent." in log_file:
                result["agent"].append(log_file)
            elif "dashboard." in log_file:
                result["dashboard"].append(log_file)
            else:
                result["internal"].append(log_file)