from functools import partial
import os
import logging
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
from googleapiclient import discovery, errors

logger = logging.getLogger(__name__)

crm = discovery.build("cloudresourcemanager", "v1")
iam = discovery.build("iam", "v1")
compute = discovery.build("compute", "v1")

VERSION = "v1"

RAY = "ray-autoscaler"
DEFAULT_SERVICE_ACCOUNT_ID = RAY + "-sa-" + VERSION
SERVICE_ACCOUNT_EMAIL_TEMPLATE = (
    "{account_id}@{project_id}.iam.gserviceaccount.com")
DEFAULT_SERVICE_ACCOUNT_CONFIG = {
    "displayName": "Ray Autoscaler Service Account ({})".format(VERSION),
}
DEFAULT_SERVICE_ACCOUNT_ROLES = ("roles/storage.objectAdmin",
                                 "roles/compute.admin")

MAX_POLLS = 12
POLL_INTERVAL = 5


def wait_for_crm_operation(operation):
    """Poll for cloud resource manager operation until finished."""
    logger.info("wait_for_crm_operation: "
                "Waiting for operation {} to finish...".format(operation))

    for _ in range(MAX_POLLS):
        result = crm.operations().get(name=operation["name"]).execute()
        if "error" in result:
            raise Exception(result["error"])

        if "done" in result and result["done"]:
            logger.info("wait_for_crm_operation: Operation done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


def wait_for_compute_global_operation(project_name, operation):
    """Poll for global compute operation until finished."""
    logger.info("wait_for_compute_global_operation: "
                "Waiting for operation {} to finish...".format(
                    operation["name"]))

    for _ in range(MAX_POLLS):
        result = compute.globalOperations().get(
            project=project_name,
            operation=operation["name"],
        ).execute()
        if "error" in result:
            raise Exception(result["error"])

        if result["status"] == "DONE":
            logger.info("wait_for_compute_global_operation: "
                        "Operation done.")
            break

        time.sleep(POLL_INTERVAL)

    return result


def key_pair_name(i, region, project_id, ssh_user):
    """Returns the ith default gcp_key_pair_name."""
    key_name = "{}_gcp_{}_{}_{}".format(RAY, region, project_id, ssh_user, i)
    return key_name


def key_pair_paths(key_name):
    """Returns public and private key paths for a given key_name."""
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))
    return public_key_path, private_key_path


def generate_rsa_key_pair():
    """Create public and private ssh-keys."""

    key = rsa.generate_private_key(
        backend=default_backend(), public_exponent=65537, key_size=2048)

    public_key = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH,
        serialization.PublicFormat.OpenSSH).decode("utf-8")

    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()).decode("utf-8")

    return public_key, pem


def bootstrap_gcp(config):
    config = _configure_project(config)
    config = _configure_iam_role(config)
    config = _configure_key_pair(config)
    config = _configure_subnet(config)

    return config


def _configure_project(config):
    """Setup a Google Cloud Platform Project.

    Google Compute Platform organizes all the resources, such as storage
    buckets, users, and instances under projects. This is different from
    aws ec2 where everything is global.
    """
    project_id = config["provider"].get("project_id")
    assert config["provider"]["project_id"] is not None, (
        "'project_id' must be set in the 'provider' section of the autoscaler"
        " config. Notice that the project id must be globally unique.")
    project = _get_project(project_id)

    if project is None:
        #  Project not found, try creating it
        _create_project(project_id)
        project = _get_project(project_id)

    assert project is not None, "Failed to create project"
    assert project["lifecycleState"] == "ACTIVE", (
        "Project status needs to be ACTIVE, got {}".format(
            project["lifecycleState"]))

    config["provider"]["project_id"] = project["projectId"]

    return config


def _configure_iam_role(config):
    """Setup a gcp service account with IAM roles.

    Creates a gcp service acconut and binds IAM roles which allow it to control
    control storage/compute services. Specifically, the head node needs to have
    an IAM role that allows it to create further gce instances and store items
    in google cloud storage.

    TODO: Allow the name/id of the service account to be configured
    """
    email = SERVICE_ACCOUNT_EMAIL_TEMPLATE.format(
        account_id=DEFAULT_SERVICE_ACCOUNT_ID,
        project_id=config["provider"]["project_id"])
    service_account = _get_service_account(email, config)

    if service_account is None:
        logger.info("_configure_iam_role: "
                    "Creating new service account {}".format(
                        DEFAULT_SERVICE_ACCOUNT_ID))

        service_account = _create_service_account(
            DEFAULT_SERVICE_ACCOUNT_ID, DEFAULT_SERVICE_ACCOUNT_CONFIG, config)

    assert service_account is not None, "Failed to create service account"

    _add_iam_policy_binding(service_account, DEFAULT_SERVICE_ACCOUNT_ROLES)

    config["head_node"]["serviceAccounts"] = [{
        "email": service_account["email"],
        # NOTE: The amount of access is determined by the scope + IAM
        # role of the service account. Even if the cloud-platform scope
        # gives (scope) access to the whole cloud-platform, the service
        # account is limited by the IAM rights specified below.
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }]

    return config


def _configure_key_pair(config):
    """Configure SSH access, using an existing key pair if possible.

    Creates a project-wide ssh key that can be used to access all the instances
    unless explicitly prohibited by instance config.

    The ssh-keys created by ray are of format:

      [USERNAME]:ssh-rsa [KEY_VALUE] [USERNAME]

    where:

      [USERNAME] is the user for the SSH key, specified in the config.
      [KEY_VALUE] is the public SSH key value.
    """

    if "ssh_private_key" in config["auth"]:
        return config

    ssh_user = config["auth"]["ssh_user"]

    project = compute.projects().get(
        project=config["provider"]["project_id"]).execute()

    # Key pairs associated with project meta data. The key pairs are general,
    # and not just ssh keys.
    ssh_keys_str = next(
        (item for item in project["commonInstanceMetadata"].get("items", [])
         if item["key"] == "ssh-keys"), {}).get("value", "")

    ssh_keys = ssh_keys_str.split("\n") if ssh_keys_str else []

    # Try a few times to get or create a good key pair.
    key_found = False
    for i in range(10):
        key_name = key_pair_name(i, config["provider"]["region"],
                                 config["provider"]["project_id"], ssh_user)
        public_key_path, private_key_path = key_pair_paths(key_name)

        for ssh_key in ssh_keys:
            key_parts = ssh_key.split(" ")
            if len(key_parts) != 3:
                continue

            if key_parts[2] == ssh_user and os.path.exists(private_key_path):
                # Found a key
                key_found = True
                break

        # Create a key since it doesn't exist locally or in GCP
        if not key_found and not os.path.exists(private_key_path):
            logger.info("_configure_key_pair: "
                        "Creating new key pair {}".format(key_name))
            public_key, private_key = generate_rsa_key_pair()

            _create_project_ssh_key_pair(project, public_key, ssh_user)

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
        ssh_user, private_key_path)
    assert os.path.exists(private_key_path), (
        "Private key file {} not found for user {}"
        "".format(private_key_path, ssh_user))

    logger.info("_configure_key_pair: "
                "Private key not specified in config, using"
                "{}".format(private_key_path))

    config["auth"]["ssh_private_key"] = private_key_path

    return config


def _configure_subnet(config):
    """Pick a reasonable subnet if not specified by the config."""

    # Rationale: avoid subnet lookup if the network is already
    # completely manually configured
    if ("networkInterfaces" in config["head_node"]
            and "networkInterfaces" in config["worker_nodes"]):
        return config

    subnets = _list_subnets(config)

    if not subnets:
        raise NotImplementedError("Should be able to create subnet.")

    # TODO: make sure that we have usable subnet. Maybe call
    # compute.subnetworks().listUsable? For some reason it didn't
    # work out-of-the-box
    default_subnet = subnets[0]

    if "networkInterfaces" not in config["head_node"]:
        config["head_node"]["networkInterfaces"] = [{
            "subnetwork": default_subnet["selfLink"],
            "accessConfigs": [{
                "name": "External NAT",
                "type": "ONE_TO_ONE_NAT",
            }],
        }]

    if "networkInterfaces" not in config["worker_nodes"]:
        config["worker_nodes"]["networkInterfaces"] = [{
            "subnetwork": default_subnet["selfLink"],
            "accessConfigs": [{
                "name": "External NAT",
                "type": "ONE_TO_ONE_NAT",
            }],
        }]

    return config


def _list_subnets(config):
    response = compute.subnetworks().list(
        project=config["provider"]["project_id"],
        region=config["provider"]["region"]).execute()

    return response["items"]


def _get_subnet(config, subnet_id):
    subnet = compute.subnetworks().get(
        project=config["provider"]["project_id"],
        region=config["provider"]["region"],
        subnetwork=subnet_id,
    ).execute()

    return subnet


def _get_project(project_id):
    try:
        project = crm.projects().get(projectId=project_id).execute()
    except errors.HttpError as e:
        if e.resp.status != 403:
            raise
        project = None

    return project


def _create_project(project_id):
    operation = crm.projects().create(body={
        "projectId": project_id,
        "name": project_id
    }).execute()

    result = wait_for_crm_operation(operation)

    return result


def _get_service_account(account, config):
    project_id = config["provider"]["project_id"]
    full_name = ("projects/{project_id}/serviceAccounts/{account}"
                 "".format(project_id=project_id, account=account))
    try:
        service_account = iam.projects().serviceAccounts().get(
            name=full_name).execute()
    except errors.HttpError as e:
        if e.resp.status != 404:
            raise
        service_account = None

    return service_account


def _create_service_account(account_id, account_config, config):
    project_id = config["provider"]["project_id"]

    service_account = iam.projects().serviceAccounts().create(
        name="projects/{project_id}".format(project_id=project_id),
        body={
            "accountId": account_id,
            "serviceAccount": account_config,
        }).execute()

    return service_account


def _add_iam_policy_binding(service_account, roles):
    """Add new IAM roles for the service account."""
    project_id = service_account["projectId"]
    email = service_account["email"]
    member_id = "serviceAccount:" + email

    policy = crm.projects().getIamPolicy(
        resource=project_id, body={}).execute()

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
            policy["bindings"].append({
                "members": [member_id],
                "role": role,
            })

    if already_configured:
        # In some managed environments, an admin needs to grant the
        # roles, so only call setIamPolicy if needed.
        return

    result = crm.projects().setIamPolicy(
        resource=project_id, body={
            "policy": policy,
        }).execute()

    return result


def _create_project_ssh_key_pair(project, public_key, ssh_user):
    """Inserts an ssh-key into project commonInstanceMetadata"""

    key_parts = public_key.split(" ")

    # Sanity checks to make sure that the generated key matches expectation
    assert len(key_parts) == 2, key_parts
    assert key_parts[0] == "ssh-rsa", key_parts

    new_ssh_meta = "{ssh_user}:ssh-rsa {key_value} {ssh_user}".format(
        ssh_user=ssh_user, key_value=key_parts[1])

    common_instance_metadata = project["commonInstanceMetadata"]
    items = common_instance_metadata.get("items", [])

    ssh_keys_i = next(
        (i for i, item in enumerate(items) if item["key"] == "ssh-keys"), None)

    if ssh_keys_i is None:
        items.append({"key": "ssh-keys", "value": new_ssh_meta})
    else:
        ssh_keys = items[ssh_keys_i]
        ssh_keys["value"] += "\n" + new_ssh_meta
        items[ssh_keys_i] = ssh_keys

    common_instance_metadata["items"] = items

    operation = compute.projects().setCommonInstanceMetadata(
        project=project["name"], body=common_instance_metadata).execute()

    response = wait_for_compute_global_operation(project["name"], operation)

    return response
