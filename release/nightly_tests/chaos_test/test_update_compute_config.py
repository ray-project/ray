import os
import requests
import anyscale
import time

cluster_id = os.environ["ANYSCALE_CLUSTER_ID"]
print(f"cluster id: {cluster_id}")

sdk = anyscale.AnyscaleSDK()
cluster = sdk.get_cluster(cluster_id)
print(f"cluster: {cluster}")

existing_compute_config = anyscale.compute_config.get(
    name="", _id=cluster.result.cluster_compute_id
).config
compute_config_dict = existing_compute_config.to_dict()
print(f"compute config {compute_config_dict}")
compute_config_dict["worker_nodes"][0]["max_nodes"] = 11
new_compute_config = anyscale.compute_config.models.ComputeConfig.from_dict(
    compute_config_dict
)
new_compute_config_name = anyscale.compute_config.create(new_compute_config, name=None)
new_compute_config_id = anyscale.compute_config.get(name=new_compute_config_name).id
print(f"new compute config {new_compute_config_id}")

response = requests.put(
    f"https://console.anyscale-staging.com/api/v2/sessions/{cluster_id}/cluster_config_with_session_idle_timeout",
    params={
        "build_id": cluster.result.cluster_environment_build_id,
        "compute_template_id": new_compute_config_id,
    },
    headers={"Authorization": f"Bearer {os.environ['ANYSCALE_CLI_TOKEN']}"},
)
print(response)

time.sleep(1000000)
