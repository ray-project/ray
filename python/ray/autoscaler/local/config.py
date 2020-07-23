def bootstrap_local(config):
    # Copy max_workers to config["provider"] in on-prem auto-management mode.
    if "server_address" in config["provider"]:
        config["provider"]["max_workers"] = config["max_workers"]
    return config
