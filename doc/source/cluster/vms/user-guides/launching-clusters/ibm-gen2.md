
# Launching Ray Clusters on IBM VPC

This guide details the steps needed to start a Ray cluster on IBM VPC.

To start a IBM VPC Ray cluster, you will use the Ray cluster launcher and
interactive config tool to generate ray cluster config file for IBM VPC.


```{note}
The IBM VPC integration is community-maintained. Please reach out to the integration maintainers on Github
if you run into any problems: kpavel.
```

## Using Ray cluster launcher


### Install Ray cluster launcher

The Ray cluster launcher is part of the `ray` CLI. Use the CLI to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install the ray CLI with cluster launcher support. Follow [the Ray installation documentation](installation) for more detailed instructions.

```bash
# install ray
pip install -U ray[default]
```

### Install ibm cloud client packages and config tool

```bash
# install dependencies
pip install ibm_vpc ibm_platform_services ibm_cloud_sdk_core
# install config tool
pip install -U lithopscloud
```

### Obtain IBM Cloud API Key

How to obtain the API key is described in the [docs](https://cloud.ibm.com/docs/account?topic=account-userapikey)

### Use config tool to generate RAY IBM VPC cluster config.yaml file 

```bash
        
    lithopscloud --iam-api-key <IAM_API_KEY> -o config.yaml -b ray --pr

    # And follow the interactive wizard

    [?] Choose region: eu-de
        au-syd
        br-sao
        ca-tor
    > eu-de
        eu-gb
        jp-osa
        jp-tok
        us-east
        us-south
```

Once the above is done, you should be ready to launch your cluster.

### Start Ray with the Ray cluster launcher

Test that it works by running the following commands from your local machine:

```bash
# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
$ ray up config.yaml

# Get a remote screen on the head node.
$ ray attach config.yaml
$ # Try running a Ray program with 'ray.init(address="auto")'.

# Tear down the cluster.
$ ray down config.yaml
```

Congratulations, you have started a Ray cluster on IBM Virtual Private Cloud!
