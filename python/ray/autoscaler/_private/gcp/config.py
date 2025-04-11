import copy
import json
import logging
import os
import re
import time
from functools import partial, reduce

import google_auth_httplib2
import googleapiclient
import httplib2
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as OAuthCredentials
from googleapiclient import discovery, errors

from ray._private.accelerators import TPUAcceleratorManager
from ray._private.accelerators import tpu
from ray.autoscaler._private.gcp.node import MAX_POLLS, POLL_INTERVAL, GCPNodeType
from ray.autoscaler._private.util import check_legacy_fields

logger = logging.getLogger(__name__)

VERSION = "v1"
TPU_VERSION = "v2alpha"  # change once v2 is stable

RAY = "ray-autoscaler"
DEFAULT_SERVICE_ACCOUNT_ID = RAY + "-sa-" + VERSION
SERVICE_ACCOUNT_EMAIL_TEMPLATE = "{account_id}@{project_id}.iam.gserviceaccount.com"
DEFAULT_SERVICE_ACCOUNT_CONFIG = {
    "displayName": "Ray Autoscaler Service Account ({})".format(VERSION),
}

# Those roles will be always added.
# NOTE: `serviceAccountUser` allows the head node to create workers with
# a serviceAccount. `roleViewer` allows the head node to run bootstrap_gcp.
DEFAULT_SERVICE_ACCOUNT_ROLES = [
    "roles/storage.objectAdmin",
    "roles/compute.admin",
    "roles/iam.serviceAccountUser",
    "roles/iam.roleViewer",
]
# Those roles will only be added if there are TPU nodes defined in config.
TPU_SERVICE_ACCOUNT_ROLES = ["roles/tpu.admin"]

# If there are TPU nodes in config, this field will be set
# to True in config["provider"].
HAS_TPU_PROVIDER_FIELD = "_has_tpus"

# NOTE: iam.serviceAccountUser allows the Head Node to create worker nodes
# with ServiceAccounts.


def tpu_accelerator_config_to_type(accelerator_config: dict) -> str:
    """Convert a provided accelerator_config to accelerator_type.

    Args:
        accelerator_config: A dictionary defining the spec of a
            TPU accelerator. The dictionary should consist of
            the keys 'type', indicating the TPU chip type, and
            'topology', indicating the topology of the TPU.

    Returns:
        A string, accelerator_type, e.g. "v4-8".

    """
    generation = accelerator_config["type"].lower()
    topology = accelerator_config["topology"]
    # Reduce e.g. "2x2x2" to 8
    chip_dimensions = [int(chip_count) for chip_count in topology.split("x")]
    num_chips = reduce(lambda x, y: x * y, chip_dimensions)

    # V5LitePod is rendered as "V5LITE_POD" in accelerator configuration but
    # accelerator type uses a format like "v5litepod-{cores}", so we need
    # to manually convert the string here.
    if generation == "v5lite_pod":
        generation = "v5litepod"

    num_cores = tpu.get_tpu_cores_per_chip(generation) * num_chips

    return f"{generation}-{num_cores}"


def _validate_tpu_config(node: dict):
    """Validate the provided node with TPU support.

    If the config is malformed, users will run into an error but this function
    will raise the error at config parsing time. This only tests very simple assertions.

    Raises: `ValueError` in case the input is malformed.

    """
    if "acceleratorType" in node and "acceleratorConfig" in node:
        raise ValueError(
            "For TPU usage, acceleratorType and acceleratorConfig "
            "cannot both be set."
        )
    if "acceleratorType" in node:
        accelerator_type = node["acceleratorType"]
        if not TPUAcceleratorManager.is_valid_tpu_accelerator_type(accelerator_type):
            raise ValueError(
                "`acceleratorType` should match v(generation)-(cores/chips). "
                f"Got {accelerator_type}."
            )
    else:  # "acceleratorConfig" in node
        accelerator_config = node["acceleratorConfig"]
        if "type" not in accelerator_config or "topology" not in accelerator_config:
            raise ValueError(
                "acceleratorConfig expects 'type' and 'topology'. "
                f"Got {accelerator_config}"
            )
        generation = node["acceleratorConfig"]["type"]
        topology = node["acceleratorConfig"]["topology"]

        generation_pattern = re.compile(r"^V\d+[a-zA-Z]*$")
        topology_pattern = re.compile(r"^\d+x\d+(x\d+)?$")

        if generation != "V5LITE_POD" and not generation_pattern.match(generation):
            raise ValueError(f"type should match V(generation). Got {generation}.")
        if generation == "V2" or generation == "V3":
            raise ValueError(
                f"acceleratorConfig is not supported on V2/V3 TPUs. Got {generation}."
            )
        if not topology_pattern.match(topology):
            raise ValueError(
                f"topology should be of form axbxc or axb. Got {topology}."
            )


def _get_num_tpu_chips(node: dict) -> int:
    chips = 0
    if "acceleratorType" in node:
        accelerator_type = node["acceleratorType"]
        # `acceleratorType` is typically v{generation}-{cores}
        cores = int(accelerator_type.split("-")[1])
        chips = cores / tpu.get_tpu_cores_per_chip(accelerator_type)
    if "acceleratorConfig" in node:
        topology = node["acceleratorConfig"]["topology"]
        # `topology` is typically {chips}x{chips}x{chips}
        # Multiply all dimensions together to get total number of chips
        chips = 1
        for dim in topology.split("x"):
            chips *= int(dim)
    return chips


def _is_single_host_tpu(node: dict) -> bool:
    accelerator_type = ""
    if "acceleratorType" in node:
        accelerator_type = node["acceleratorType"]
    else:
        accelerator_type = tpu_accelerator_config_to_type(node["acceleratorConfig"])
    return _get_num_tpu_chips(node) <= tpu.get_num_tpu_visible_chips_per_host(
        accelerator_type
    )


def get_node_type(node: dict) -> GCPNodeType:
    """Returns node type based on the keys in ``node``.

    This is a very simple check. If we have a ``machineType`` key,
    this is a Compute instance. If we don't have a ``machineType`` key,
    but we have ``acceleratorType``, this is a TPU. Otherwise, it's
    invalid and an exception is raised.

    This works for both node configs and API returned nodes.
    """

    if (
        "machineType" not in node
        and "acceleratorType" not in node
        and "acceleratorConfig" not in node
    ):
        raise ValueError(
            "Invalid node. For a Compute instance, 'machineType' is required."
            "For a TPU instance, 'acceleratorType' OR 'acceleratorConfig' and "
            f"no 'machineType' is required. Got {list(node)}."
        )

    if "machineType" not in node and (
        "acceleratorType" in node or "acceleratorConfig" in node
    ):
        _validate_tpu_config(node)
        if not _is_single_host_tpu(node):
            # Remove once proper autoscaling support is added.
            logger.warning(
                "TPU pod detected. Note that while the cluster launcher can create "
                "multiple TPU pods, proper autoscaling will not work as expected, "
                "as all hosts in a TPU pod need to execute the same program. "
                "Proceed with caution."
            )
        return GCPNodeType.TPU
    return GCPNodeType.COMPUTE


def wait_for_crm_operation(operation, crm):
    """Poll for cloud resource manager operation until finished."""
    logger.info(
        "wait_for_crm_operation: "
        "Waiting for operation {} to finish...".format(operation)
    )

    for _ in range(MAX_POLLS):
        result = crm.operations().get(name=operation["name"]).execute()
        if "error" in result:
            raise Exception(result["error"])

        if "done" in result and result["done"]:
            logger.info("wait_for_crm_operation: Operation done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


def wait_for_compute_global_operation(project_name, operation, compute):
    """Poll for global compute operation until finished."""
    logger.info(
        "wait_for_compute_global_operation: "
        "Waiting for operation {} to finish...".format(operation["name"])
    )

    for _ in range(MAX_POLLS):
        result = (
            compute.globalOperations()
            .get(
                project=project_name,
                operation=operation["name"],
            )
            .execute()
        )
        if "error" in result:
            raise Exception(result["error"])

        if result["status"] == "DONE":
            logger.info("wait_for_compute_global_operation: Operation done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


def key_pair_name(i, region, project_id, ssh_user):
    """Returns the ith default gcp_key_pair_name."""
    key_name = "{}_gcp_{}_{}_{}_{}".format(RAY, region, project_id, ssh_user, i)
    return key_name


def key_pair_paths(key_name):
    """Returns public and private key paths for a given key_name."""
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))
    return public_key_path, private_key_path


def generate_rsa_key_pair():
    """Create public and private ssh-keys."""

    key = rsa.generate_private_key(
        backend=default_backend(), public_exponent=65537, key_size=2048
    )

    public_key = (
        key.public_key()
        .public_bytes(
            serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
        )
        .decode("utf-8")
    )

    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    return public_key, pem


def _has_tpus_in_node_configs(config: dict) -> bool:
    """Check if any nodes in config are TPUs."""
    node_configs = [
        node_type["node_config"]
        for node_type in config["available_node_types"].values()
    ]
    return any(get_node_type(node) == GCPNodeType.TPU for node in node_configs)


def _is_head_node_a_tpu(config: dict) -> bool:
    """Check if the head node is a TPU."""
    node_configs = {
        node_id: node_type["node_config"]
        for node_id, node_type in config["available_node_types"].items()
    }
    return get_node_type(node_configs[config["head_node_type"]]) == GCPNodeType.TPU


def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(
        http.credentials, http=httplib2.Http()
    )
    return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)


def _create_crm(gcp_credentials=None):
    return discovery.build(
        "cloudresourcemanager",
        "v1",
        credentials=gcp_credentials,
        requestBuilder=build_request,
        cache_discovery=False,
    )


def _create_iam(gcp_credentials=None):
    return discovery.build(
        "iam",
        "v1",
        credentials=gcp_credentials,
        requestBuilder=build_request,
        cache_discovery=False,
    )


def _create_compute(gcp_credentials=None):
    return discovery.build(
        "compute",
        "v1",
        credentials=gcp_credentials,
        requestBuilder=build_request,
        cache_discovery=False,
    )


def _create_tpu(gcp_credentials=None):
    return discovery.build(
        "tpu",
        TPU_VERSION,
        credentials=gcp_credentials,
        requestBuilder=build_request,
        cache_discovery=False,
        discoveryServiceUrl="https://tpu.googleapis.com/$discovery/rest",
    )


def construct_clients_from_provider_config(provider_config):
    """
    Attempt to fetch and parse the JSON GCP credentials from the provider
    config yaml file.

    tpu resource (the last element of the tuple) will be None if
    `_has_tpus` in provider config is not set or False.
    """
    gcp_credentials = provider_config.get("gcp_credentials")
    if gcp_credentials is None:
        logger.debug(
            "gcp_credentials not found in cluster yaml file. "
            "Falling back to GOOGLE_APPLICATION_CREDENTIALS "
            "environment variable."
        )
        tpu_resource = (
            _create_tpu()
            if provider_config.get(HAS_TPU_PROVIDER_FIELD, False)
            else None
        )
        # If gcp_credentials is None, then discovery.build will search for
        # credentials in the local environment.
        return _create_crm(), _create_iam(), _create_compute(), tpu_resource

    assert (
        "type" in gcp_credentials
    ), "gcp_credentials cluster yaml field missing 'type' field."
    assert (
        "credentials" in gcp_credentials
    ), "gcp_credentials cluster yaml field missing 'credentials' field."

    cred_type = gcp_credentials["type"]
    credentials_field = gcp_credentials["credentials"]

    if cred_type == "service_account":
        # If parsing the gcp_credentials failed, then the user likely made a
        # mistake in copying the credentials into the config yaml.
        try:
            service_account_info = json.loads(credentials_field)
        except json.decoder.JSONDecodeError:
            raise RuntimeError(
                "gcp_credentials found in cluster yaml file but "
                "formatted improperly."
            )
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
    elif cred_type == "credentials_token":
        # Otherwise the credentials type must be credentials_token.
        credentials = OAuthCredentials(credentials_field)

    tpu_resource = (
        _create_tpu(credentials)
        if provider_config.get(HAS_TPU_PROVIDER_FIELD, False)
        else None
    )

    return (
        _create_crm(credentials),
        _create_iam(credentials),
        _create_compute(credentials),
        tpu_resource,
    )


def bootstrap_gcp(config):
    config = copy.deepcopy(config)
    check_legacy_fields(config)
    # Used internally to store head IAM role.
    config["head_node"] = {}

    # Check if we have any TPUs defined, and if so,
    # insert that information into the provider config
    if _has_tpus_in_node_configs(config):
        config["provider"][HAS_TPU_PROVIDER_FIELD] = True

    crm, iam, compute, tpu = construct_clients_from_provider_config(config["provider"])

    config = _configure_project(config, crm)
    config = _configure_iam_role(config, crm, iam)
    config = _configure_key_pair(config, compute)
    config = _configure_subnet(config, compute)

    return config


def _configure_project(config, crm):
    """Setup a Google Cloud Platform Project.

    Google Compute Platform organizes all the resources, such as storage
    buckets, users, and instances under projects. This is different from
    aws ec2 where everything is global.
    """
    config = copy.deepcopy(config)

    project_id = config["provider"].get("project_id")
    assert config["provider"]["project_id"] is not None, (
        "'project_id' must be set in the 'provider' section of the autoscaler"
        " config. Notice that the project id must be globally unique."
    )
    project = _get_project(project_id, crm)

    if project is None:
        #  Project not found, try creating it
        _create_project(project_id, crm)
        project = _get_project(project_id, crm)

    assert project is not None, "Failed to create project"
    assert (
        project["lifecycleState"] == "ACTIVE"
    ), "Project status needs to be ACTIVE, got {}".format(project["lifecycleState"])

    config["provider"]["project_id"] = project["projectId"]

    return config


def _configure_iam_role(config, crm, iam):
    """Setup a gcp service account with IAM roles.

    Creates a gcp service acconut and binds IAM roles which allow it to control
    control storage/compute services. Specifically, the head node needs to have
    an IAM role that allows it to create further gce instances and store items
    in google cloud storage.

    TODO: Allow the name/id of the service account to be configured
    """
    config = copy.deepcopy(config)

    email = SERVICE_ACCOUNT_EMAIL_TEMPLATE.format(
        account_id=DEFAULT_SERVICE_ACCOUNT_ID,
        project_id=config["provider"]["project_id"],
    )
    service_account = _get_service_account(email, config, iam)

    if service_account is None:
        logger.info(
            "_configure_iam_role: "
            "Creating new service account {}".format(DEFAULT_SERVICE_ACCOUNT_ID)
        )

        service_account = _create_service_account(
            DEFAULT_SERVICE_ACCOUNT_ID, DEFAULT_SERVICE_ACCOUNT_CONFIG, config, iam
        )

    assert service_account is not None, "Failed to create service account"

    if config["provider"].get(HAS_TPU_PROVIDER_FIELD, False):
        roles = DEFAULT_SERVICE_ACCOUNT_ROLES + TPU_SERVICE_ACCOUNT_ROLES
    else:
        roles = DEFAULT_SERVICE_ACCOUNT_ROLES

    _add_iam_policy_binding(service_account, roles, crm)

    config["head_node"]["serviceAccounts"] = [
        {
            "email": service_account["email"],
            # NOTE: The amount of access is determined by the scope + IAM
            # role of the service account. Even if the cloud-platform scope
            # gives (scope) access to the whole cloud-platform, the service
            # account is limited by the IAM rights specified below.
            "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        }
    ]

    return config


def _configure_key_pair(config, compute):
    """Configure SSH access, using an existing key pair if possible.

    Creates a project-wide ssh key that can be used to access all the instances
    unless explicitly prohibited by instance config.

    The ssh-keys created by ray are of format:

      [USERNAME]:ssh-rsa [KEY_VALUE] [USERNAME]

    where:

      [USERNAME] is the user for the SSH key, specified in the config.
      [KEY_VALUE] is the public SSH key value.
    """
    config = copy.deepcopy(config)

    if "ssh_private_key" in config["auth"]:
        return config

    ssh_user = config["auth"]["ssh_user"]

    project = compute.projects().get(project=config["provider"]["project_id"]).execute()

    # Key pairs associated with project meta data. The key pairs are general,
    # and not just ssh keys.
    ssh_keys_str = next(
        (
            item
            for item in project["commonInstanceMetadata"].get("items", [])
            if item["key"] == "ssh-keys"
        ),
        {},
    ).get("value", "")

    ssh_keys = ssh_keys_str.split("\n") if ssh_keys_str else []

    # Try a few times to get or create a good key pair.
    key_found = False
    for i in range(10):
        key_name = key_pair_name(
            i, config["provider"]["region"], config["provider"]["project_id"], ssh_user
        )
        public_key_path, private_key_path = key_pair_paths(key_name)

        for ssh_key in ssh_keys:
            key_parts = ssh_key.split(" ")
            if len(key_parts) != 3:
                continue

            if key_parts[2] == ssh_user and os.path.exists(private_key_path):
                # Found a key
                key_found = True
                break

        # Writing the new ssh key to the filesystem fails if the ~/.ssh
        # directory doesn't already exist.
        os.makedirs(os.path.expanduser("~/.ssh"), exist_ok=True)

        # Create a key since it doesn't exist locally or in GCP
        if not key_found and not os.path.exists(private_key_path):
            logger.info(
                "_configure_key_pair: Creating new key pair {}".format(key_name)
            )
            public_key, private_key = generate_rsa_key_pair()

            _create_project_ssh_key_pair(project, public_key, ssh_user, compute)

            # Create the directory if it doesn't exists
            private_key_dir = os.path.dirname(private_key_path)
            os.makedirs(private_key_dir, exist_ok=True)

            # We need to make sure to _create_ the file with the right
            # permissions. In order to do that we need to change the default
            # os.open behavior to include the mode we want.
            with open(
                private_key_path,
                "w",
                opener=partial(os.open, mode=0o600),
            ) as f:
                f.write(private_key)

            with open(public_key_path, "w") as f:
                f.write(public_key)

            key_found = True

            break

        if key_found:
            break

    assert key_found, "SSH keypair for user {} not found for {}".format(
        ssh_user, private_key_path
    )
    assert os.path.exists(
        private_key_path
    ), "Private key file {} not found for user {}".format(private_key_path, ssh_user)

    logger.info(
        "_configure_key_pair: "
        "Private key not specified in config, using"
        "{}".format(private_key_path)
    )

    config["auth"]["ssh_private_key"] = private_key_path

    return config


def _configure_subnet(config, compute):
    """Pick a reasonable subnet if not specified by the config."""
    config = copy.deepcopy(config)

    node_configs = [
        node_type["node_config"]
        for node_type in config["available_node_types"].values()
    ]
    # Rationale: avoid subnet lookup if the network is already
    # completely manually configured

    # networkInterfaces is compute, networkConfig is TPU
    if all(
        "networkInterfaces" in node_config or "networkConfig" in node_config
        for node_config in node_configs
    ):
        return config

    subnets = _list_subnets(config, compute)

    if not subnets:
        raise NotImplementedError("Should be able to create subnet.")

    # TODO: make sure that we have usable subnet. Maybe call
    # compute.subnetworks().listUsable? For some reason it didn't
    # work out-of-the-box
    default_subnet = subnets[0]

    default_interfaces = [
        {
            "subnetwork": default_subnet["selfLink"],
            "accessConfigs": [
                {
                    "name": "External NAT",
                    "type": "ONE_TO_ONE_NAT",
                }
            ],
        }
    ]

    for node_config in node_configs:
        # The not applicable key will be removed during node creation

        # compute
        if "networkInterfaces" not in node_config:
            node_config["networkInterfaces"] = copy.deepcopy(default_interfaces)
        # TPU
        if "networkConfig" not in node_config:
            node_config["networkConfig"] = copy.deepcopy(default_interfaces)[0]
            node_config["networkConfig"].pop("accessConfigs")

    return config


def _list_subnets(config, compute):
    response = (
        compute.subnetworks()
        .list(
            project=config["provider"]["project_id"],
            region=config["provider"]["region"],
        )
        .execute()
    )

    return response["items"]


def _get_subnet(config, subnet_id, compute):
    subnet = (
        compute.subnetworks()
        .get(
            project=config["provider"]["project_id"],
            region=config["provider"]["region"],
            subnetwork=subnet_id,
        )
        .execute()
    )

    return subnet


def _get_project(project_id, crm):
    try:
        project = crm.projects().get(projectId=project_id).execute()
    except errors.HttpError as e:
        if e.resp.status != 403:
            raise
        project = None

    return project


def _create_project(project_id, crm):
    operation = (
        crm.projects()
        .create(body={"projectId": project_id, "name": project_id})
        .execute()
    )

    result = wait_for_crm_operation(operation, crm)

    return result


def _get_service_account(account, config, iam):
    project_id = config["provider"]["project_id"]
    full_name = "projects/{project_id}/serviceAccounts/{account}".format(
        project_id=project_id, account=account
    )
    try:
        service_account = iam.projects().serviceAccounts().get(name=full_name).execute()
    except errors.HttpError as e:
        if e.resp.status != 404:
            raise
        service_account = None

    return service_account


def _create_service_account(account_id, account_config, config, iam):
    project_id = config["provider"]["project_id"]

    service_account = (
        iam.projects()
        .serviceAccounts()
        .create(
            name="projects/{project_id}".format(project_id=project_id),
            body={
                "accountId": account_id,
                "serviceAccount": account_config,
            },
        )
        .execute()
    )

    return service_account


def _add_iam_policy_binding(service_account, roles, crm):
    """Add new IAM roles for the service account."""
    project_id = service_account["projectId"]
    email = service_account["email"]
    member_id = "serviceAccount:" + email

    policy = (
        crm.projects()
        .getIamPolicy(
            resource=project_id, body={"options": {"requestedPolicyVersion": 3}}
        )
        .execute()
    )

    already_configured = True
    for role in roles:
        role_exists = False
        for binding in policy["bindings"]:
            if binding["role"] == role:
                if member_id not in binding["members"]:
                    binding["members"].append(member_id)
                    already_configured = False
                role_exists = True

        if not role_exists:
            already_configured = False
            policy["bindings"].append(
                {
                    "members": [member_id],
                    "role": role,
                }
            )

    if already_configured:
        # In some managed environments, an admin needs to grant the
        # roles, so only call setIamPolicy if needed.
        return

    result = (
        crm.projects()
        .setIamPolicy(
            resource=project_id,
            body={
                "policy": policy,
            },
        )
        .execute()
    )

    return result


def _create_project_ssh_key_pair(project, public_key, ssh_user, compute):
    """Inserts an ssh-key into project commonInstanceMetadata"""

    key_parts = public_key.split(" ")

    # Sanity checks to make sure that the generated key matches expectation
    assert len(key_parts) == 2, key_parts
    assert key_parts[0] == "ssh-rsa", key_parts

    new_ssh_meta = "{ssh_user}:ssh-rsa {key_value} {ssh_user}".format(
        ssh_user=ssh_user, key_value=key_parts[1]
    )

    common_instance_metadata = project["commonInstanceMetadata"]
    items = common_instance_metadata.get("items", [])

    ssh_keys_i = next(
        (i for i, item in enumerate(items) if item["key"] == "ssh-keys"), None
    )

    if ssh_keys_i is None:
        items.append({"key": "ssh-keys", "value": new_ssh_meta})
    else:
        ssh_keys = items[ssh_keys_i]
        ssh_keys["value"] += "\n" + new_ssh_meta
        items[ssh_keys_i] = ssh_keys

    common_instance_metadata["items"] = items

    operation = (
        compute.projects()
        .setCommonInstanceMetadata(
            project=project["name"], body=common_instance_metadata
        )
        .execute()
    )

    response = wait_for_compute_global_operation(project["name"], operation, compute)

    return response
