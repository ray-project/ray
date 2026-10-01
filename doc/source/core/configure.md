---
myst:
  html_meta:
    description: "How to configure Ray from the Python API (ray.init) and the command line (ray start), including cluster resource overrides and other runtime settings."
---

(configuring-ray)=

# Configure Ray

:::{note}
To run Java applications, see [Java applications](#java-applications).
:::

This page describes how to configure Ray from the Python API and from the command line. For a complete overview of the configuration options, see the `ray.init` {doc}`documentation <api/index>`.

:::{important}
In a multi-node setting, you must first run `ray start` on the command line to start the Ray cluster services on the machine, and then call `ray.init` in Python to connect to those services. On a single machine, you can run `ray.init()` without `ray start`, because `ray.init()` both starts the Ray cluster services and connects to them.
:::


(cluster-resources)=

## Cluster resources

Ray detects available resources by default.

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray

# This automatically detects available resources in the single machine.
ray.init()
```

If you aren't connecting to an existing cluster, you can override cluster resources through `ray.init`:

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
# If not connecting to an existing cluster, you can specify resources overrides:
ray.init(num_cpus=8, num_gpus=1)
```

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
# Specifying custom resources
ray.init(num_gpus=1, resources={'Resource1': 4, 'Resource2': 16})
```

When you start Ray from the command line, pass the `--num-cpus` and `--num-gpus` flags into `ray start`. You can also specify custom resources.

```bash
# To start a head node.
$ ray start --head --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

# To start a non-head node.
$ ray start --address=<address> --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

# Specifying custom resources
ray start [--head] --num-cpus=<NUM_CPUS> --resources='{"Resource1": 4, "Resource2": 16}'
```

If you started Ray from the command line, connect to the Ray cluster as follows:

```{testcode}
:skipif: True

# Connect to ray. Notice if connected to existing cluster, you don't specify resources.
ray.init(address=<address>)
```

(worker-grpc-thread-configuration)=

## Worker gRPC threads on high-CPU nodes

Each Ray worker process has its own gRPC runtime. By default, each runtime assumes it owns the whole machine and sizes its internal threads from the machine's CPU count. On nodes with many worker processes, this can create a high aggregate thread count.

To reduce the CPU-count hint that each worker-side gRPC runtime uses, set `RAY_worker_num_grpc_internal_threads` to a positive integer before you start Ray. Set it on every Ray node where you want the setting to apply. The following example sets it on a head node and a worker node:

```bash
# Head node.
RAY_worker_num_grpc_internal_threads=4 ray start --head

# Worker node.
RAY_worker_num_grpc_internal_threads=4 ray start --address=<HEAD_ADDRESS>
```

The best value depends on the workload. Test small values such as 1, 2, and 4 while measuring task throughput and RPC latency.

To diagnose high worker thread counts, see {ref}`debug-worker-thread-count`.

(temp-dir-log-files)=

## Logging and debugging

Each Ray session has a unique name. By default, the name is `session_{timestamp}_{pid}`. The format of `timestamp` is `%Y-%m-%d_%H-%M-%S_%f`. For details, see [Python time format](https://strftime.org/). The PID belongs to the startup process, which is either the process that calls `ray.init()` or the Ray process that a shell executes in `ray start`.

For each session, Ray places all its temporary files under the *session directory*. A session directory is a subdirectory of the *root temporary path*, which is `/tmp/ray` by default, so the default session directory is `/tmp/ray/{ray_session_name}`. To find the latest session, sort the session directories by name.

To change the *root temporary directory*, pass `--temp-dir={your temp path}` to `ray start`.

There currently isn't a stable way to change the root temporary directory when you call `ray.init()`. If you need to, pass the `_temp_dir` argument to `ray.init()`.

For more details, see {ref}`logging directory structure <logging-directory-structure>`.

(ray-ports)=

## Ports configurations

Ray requires bidirectional communication among the nodes in a cluster. Each node opens specific ports to receive incoming network requests.

### All nodes

The following options specify the raylet ports on every node:

- `--node-manager-port`: Port for the raylet's node manager. Default: Random value.
- `--object-manager-port`: Port for the raylet's object manager. Default: Random value.
- `--runtime-env-agent-port`: Port for the raylet's runtime environment agent. Default: Random value.

The node manager and object manager run as separate processes with their own ports for communication.

The following options specify the ports that the dashboard agent process uses:

- `--dashboard-agent-grpc-port`: The port to listen on for gRPC. Default: Random value.
- `--dashboard-agent-listen-port`: The port to listen on for HTTP. Default: 52365.
- `--metrics-export-port`: The port to use to expose Ray metrics. Default: Random value.

Worker processes across machines use a range of ports, and all ports in the range should be open. The following options specify that range:

- `--min-worker-port`: Minimum port number for the worker to bind to. Default: 10002.
- `--max-worker-port`: Maximum port number for the worker to bind to. Default: 19999.

Ray uses port numbers to tell apart the input and output of multiple workers on a single node. Each worker takes input and gives output on a single port number. Therefore, by default, each node has a maximum of 10,000 workers, regardless of the number of CPUs.

In general, give Ray a wide range of possible worker ports, in case another program on your machine is using some of those ports. When debugging, though, it's useful to specify a short list of worker ports, such as `--worker-port-list=10000,10001,10002,10003,10004`. A short list limits the number of workers, the same as a narrow range does.

Each raylet hands out its worker ports in random order, so don't rely on the first worker binding to `--min-worker-port` or to the first entry of `--worker-port-list`. Randomizing keeps raylets that share a network namespace from all starting at the same end of the range, but it only lowers the odds of a collision. If you run several raylets on one host, give each one a non-overlapping port range, or pass `--min-worker-port=0 --max-worker-port=0` so that each worker binds port 0 and the OS assigns a free port. Because `ray start` defaults to `10002-19999`, omitting these options doesn't select the port 0 behavior.

### Head node

In addition to the ports in the preceding section, the head node needs to open the following ports:

- `--port`: Port of the Ray GCS server. The head node starts a GCS server listening on this port. Default: 6379.
- `--ray-client-server-port`: Listening port for the Ray Client server. Default: 10001.
- `--redis-shard-ports`: Comma-separated list of ports for non-primary Redis shards. Default: Random values.
- `--dashboard-grpc-port`: Deprecated and no longer used. Kept only for backward compatibility.

If `--include-dashboard` is true, which is the default, the head node must also open `--dashboard-port`, which defaults to 8265.

If `--include-dashboard` is true but `--dashboard-port` isn't open on the head node, you can't access the dashboard, and you repeatedly get the following error:

```bash
WARNING worker.py:1114 -- The agent on node <hostname of node that tried to run a task> failed with the following error:
Traceback (most recent call last):
  File "/usr/local/lib/python3.8/dist-packages/grpc/aio/_call.py", line 285, in __await__
    raise _create_rpc_error(self._cython_call._initial_metadata,
grpc.aio._call.AioRpcError: <AioRpcError of RPC that terminated with:
  status = StatusCode.UNAVAILABLE
  details = "failed to connect to all addresses"
  debug_error_string = "{"description":"Failed to pick subchannel","file":"src/core/ext/filters/client_channel/client_channel.cc","file_line":4165,"referenced_errors":[{"description":"failed to connect to all addresses","file":"src/core/ext/filters/client_channel/lb_policy/pick_first/pick_first.cc","file_line":397,"grpc_status":14}]}"
```

If you see that error, check whether `--dashboard-port` is accessible with `nc`, `nmap`, or your browser. The following example uses `nmap`:

```bash
$ nmap -sV --reason -p 8265 $HEAD_ADDRESS
Nmap scan report for compute04.berkeley.edu (123.456.78.910)
Host is up, received reset ttl 60 (0.00065s latency).
rDNS record for 123.456.78.910: compute04.berkeley.edu
PORT     STATE SERVICE REASON         VERSION
8265/tcp open  http    syn-ack ttl 60 aiohttp 3.7.2 (Python 3.8)
Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .
```

The dashboard runs as a separate subprocess that can crash invisibly in the background. Even if you checked port 8265 earlier, the port might have closed since then because no service is running on it anymore. This also means that if you run `ray stop` and `ray start` when the port is unreachable, the port might become reachable again because the dashboard restarts.


If you don't want the dashboard, set `--include-dashboard=false`.

## TLS authentication

You can configure Ray to use TLS on its gRPC channels. With TLS, connecting to the Ray head requires an appropriate set of credentials, and the data that the client, head, and worker processes exchange is encrypted.

TLS uses a private key and a public key for encryption and decryption. The owner keeps the private key secret and TLS shares the public key with the other party. This pattern ensures that only the intended recipient can read the message.

A Certificate Authority (CA) is a trusted third party that certifies the identity of the public key owner. The digital certificate issued by the CA contains the public key itself, the identity of the public key owner, and the expiration date of the certificate. If the owner of the public key doesn't want to obtain a digital certificate from a CA, they can generate a self-signed certificate with a tool such as OpenSSL.

To obtain a digital certificate, the owner of the public key must generate a Certificate Signing Request (CSR). The CSR contains information about the owner of the public key and the public key itself. Ray requires additional steps to set up TLS encryption.

The following steps add TLS authentication to a static Ray cluster on Kubernetes using self-signed certificates.

### Step 1: Generate a private key and self-signed certificate for CA

```bash
openssl req -x509 \
            -sha256 -days 3650 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=*.ray.io/C=US/L=San Francisco" \
            -keyout ca.key -out ca.crt
```

Use the following commands to encode the private key file and the self-signed certificate file, and then paste the encoded strings into `secret.yaml`:

```bash
cat ca.key | base64
cat ca.crt | base64
```

Alternatively, the following command encodes the CA key pair and creates the secret for it automatically:

```bash
kubectl create secret generic ca-tls --from-file=ca.crt=<path-to-ca.crt> --from-file=ca.key=<path-to-ca.key>
```

### Step 2: Generate individual private keys and self-signed certificates for the Ray head and workers

The [YAML file](https://raw.githubusercontent.com/ray-project/ray/master/doc/source/cluster/kubernetes/configs/static-ray-cluster.tls.yaml) has a ConfigMap named `tls` that includes two shell scripts, `gencert_head.sh` and `gencert_worker.sh`. These scripts produce the private key and self-signed certificate files, `tls.key` and `tls.crt`, for both head and worker Pods in the `initContainer` of each Deployment. Because the scripts run in the `initContainer`, they can dynamically retrieve the `POD_IP` and add it to the `[alt_names]` section.

The scripts perform the following steps:

1. Generate a 2048-bit RSA private key and save it as `/etc/ray/tls/tls.key`.
1. Generate a CSR from the `tls.key` file and the `csr.conf` configuration file.
1. Create a self-signed certificate, `tls.crt`, from the CA key pair and the CSR. The CA key pair is `ca.key` and `ca.crt`, and the CSR is `ca.csr`.

### Step 3: Set the environment variables for both Ray head and worker to enable TLS

Enable TLS by setting the following environment variables:

- `RAY_USE_TLS`: Set to 1 to use TLS or 0 to not use it. If you set it to 1, you must also set the other three variables in this list. Default: 0.
- `RAY_TLS_SERVER_CERT`: Location of a certificate file, `tls.crt`, which Ray presents to other endpoints to achieve mutual authentication.
- `RAY_TLS_SERVER_KEY`: Location of a private key file, `tls.key`, which is the cryptographic means to prove to other endpoints that you are the authorized user of a given certificate.
- `RAY_TLS_CA_CERT`: Location of a CA certificate file, `ca.crt`, which TLS uses to decide whether the correct authority signed the endpoint's certificate.

### Step 4: Verify TLS authentication

```bash
# Log in to the worker Pod
kubectl exec -it ${WORKER_POD} -- bash

# Since the head Pod has the certificate of the full qualified DNS resolution for the Ray head service, the connection to the worker Pods
# is established successfully
ray health-check --address service-ray-head.default.svc.cluster.local:6379

# Since service-ray-head hasn't added to the alt_names section in the certificate, the connection fails and an error
# message similar to the following is displayed: "Peer name service-ray-head is not in peer certificate".
ray health-check --address service-ray-head:6379

# After you add `DNS.3 = service-ray-head` to the alt_names sections and deploy the YAML again, the connection is able to work.
```


Enabling TLS reduces performance because of the extra overhead of mutual authentication and encryption. Testing has shown that this overhead is large for small workloads and becomes relatively smaller for large workloads. The exact overhead depends on the nature of your workload.

## Java applications

:::{important}
In a multi-node setting, you must first run `ray start` on the command line to start the Ray cluster services on the machine, and then call `ray.init()` in Java to connect to those services. On a single machine, you can run `ray.init()` without `ray start`, because `ray.init()` both starts the Ray cluster services and connects to them.
:::

(code_search_path)=

### Code search path

To run a Java application in a multi-node cluster, you must specify the code search path in your driver. The code search path tells Ray where to load JAR files from when it starts Java workers. Before you run your code, you must distribute your JAR files to the same paths on all nodes of the Ray cluster.

```bash
$ java -classpath <classpath> \
    -Dray.address=<address> \
    -Dray.job.code-search-path=/path/to/jars/ \
    <classname> <args>
```

The `/path/to/jars/` path points to a directory that contains JAR files. Workers load all JAR files in the directory. You can also provide multiple directories for this parameter:

```bash
$ java -classpath <classpath> \
    -Dray.address=<address> \
    -Dray.job.code-search-path=/path/to/jars1:/path/to/jars2:/path/to/pys1:/path/to/pys2 \
    <classname> <args>
```

You don't need to configure the code search path if you run a Java application in a single-node cluster.

For more information, see `ray.job.code-search-path` under {ref}`Driver options <java-driver-options>`.

:::{note}
Currently, there's no way to configure Ray when you run a Java application in single machine mode. If you need to configure Ray, run `ray start` first to start the Ray cluster.
:::

(java-driver-options)=

### Driver options

Java drivers have a limited set of options. These options configure only the driver, not the Ray cluster.

Ray uses [Typesafe Config](https://lightbend.github.io/config/) to read options. You can set options in two ways:

- System properties. Set a system property either by adding an option in the format `-Dkey=value` to the driver command line, or by calling `System.setProperty("key", "value");` before `Ray.init()`.
- A [HOCON format](https://github.com/lightbend/config/blob/master/HOCON.md) configuration file. By default, Ray tries to read the file named `ray.conf` in the root of the classpath. To change the location of the file, set the system property `ray.config-file` to the path of the file.

:::{note}
Options that you set as system properties take priority over options in the configuration file.
:::

The following driver options are available:

- `ray.address`

  - The cluster address if the driver connects to an existing Ray cluster. If it's empty, Ray creates a new Ray cluster.
  - Type: `String`
  - Default: Empty string

- `ray.job.code-search-path`

  - The paths for Java workers to load code from. Currently, Ray only supports directories. You can specify one or more directories separated by `:`. You don't need to configure the code search path if you run a Java application in single machine mode or local mode. If you specify a code search path, Ray also uses it to load Python code. You must set this parameter to use {ref}`cross_language`. If you specify a code search path, you can only run Python remote functions that are in the code search path.
  - Type: `String`
  - Default: Empty string
  - Example: `/path/to/jars1:/path/to/jars2:/path/to/pys1:/path/to/pys2`

- `ray.job.namespace`

  - The namespace of this job. Ray uses it to isolate jobs. Jobs in different namespaces can't access each other. If you don't specify it, Ray uses a random value.
  - Type: `String`
  - Default: A random UUID string value
