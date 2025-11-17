(kuberay-tls)=

# TLS Authentication

Ray can be configured to use TLS on its gRPC channels. This means that
connecting to the Ray head will require an appropriate
set of credentials and also that data exchanged between various processes
(client, head, workers) will be encrypted ([Ray's document](https://docs.ray.io/en/latest/ray-core/configure.html?highlight=tls#tls-authentication)).

This document provides detailed instructions for generating a public-private
key pair and CA certificate for configuring KubeRay.

> Warning: Enabling TLS will cause a performance hit due to the extra
overhead of mutual authentication and encryption. Testing has shown that
this overhead is large for small workloads and becomes relatively smaller
for large workloads. The exact overhead will depend on the nature of your
workload.

# Prerequisites

To fully understand this document, it's highly recommended that you have a
solid understanding of the following concepts:

* private/public key
* CA (certificate authority)
* CSR (certificate signing request)
* self-signed certificate

This [YouTube video](https://youtu.be/T4Df5_cojAs) is a good start.

# TL;DR

> Please note that this document is designed to support KubeRay version 0.5.0 or later. If you are using an older version of KubeRay, some of the instructions or configurations may not apply or may require additional modifications.

> Warning: Please note that the `ray-cluster.tls.yaml` file is intended for demo purposes only. It is crucial that you **do not** store
your CA private key in a Kubernetes Secret in your production environment.

```sh
# Install KubeRay operator
# `ray-cluster.tls.yaml` will cover from Step 1 to Step 3

# Download `ray-cluster.tls.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.tls.yaml

# Create a RayCluster
kubectl apply -f ray-cluster.tls.yaml

# Jump to Step 4 "Verify TLS authentication" to verify the connection.
```

`ray-cluster.tls.yaml` will create:

* A Kubernetes Secret containing the CA's private key (`ca.key`) and self-signed certificate (`ca.crt`) (**Step 1**)
* A Kubernetes ConfigMap containing the scripts `gencert_head.sh` and `gencert_worker.sh`, which allow Ray Pods to generate private keys
(`tls.key`) and self-signed certificates (`tls.crt`) (**Step 2**)
* A RayCluster with proper TLS environment variables configurations (**Step 3**)

The certificate (`tls.crt`) for a Ray Pod is encrypted using the CA's private key (`ca.key`). Additionally, all Ray Pods have the CA's public key included in `ca.crt`, which allows them to decrypt certificates from other Ray Pods.

# Step 1: Generate a private key and self-signed certificate for CA

In this document, a self-signed certificate is used, but users also have the
option to choose a publicly trusted certificate authority (CA) for their TLS
authentication.

```sh
# Step 1-1: Generate a self-signed certificate and a new private key file for CA.
openssl req -x509 \
            -sha256 -days 3650 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=*.kuberay.com/C=US/L=San Francisco" \
            -keyout ca.key -out ca.crt

# Step 1-2: Check the CA's public key from the self-signed certificate.
openssl x509 -in ca.crt -noout -text

# Step 1-3
# Method 1: Use `cat $FILENAME | base64` to encode `ca.key` and `ca.crt`.
#           Then, paste the encoding strings to the Kubernetes Secret in `ray-cluster.tls.yaml`.

# Method 2: Use kubectl to encode the certificate as Kubernetes Secret automatically.
#           (Note: You should comment out the Kubernetes Secret in `ray-cluster.tls.yaml`.)
kubectl create secret generic ca-tls --from-file=ca.key --from-file=ca.crt
```

* `ca.key`: CA's private key
* `ca.crt`: CA's self-signed certificate

This step is optional because the `ca.key` and `ca.crt` files have
already been included in the Kubernetes Secret specified in [ray-cluster.tls.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.tls.yaml).

# Step 2: Create separate private key and self-signed certificate for Ray Pods

In [ray-cluster.tls.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.tls.yaml), each Ray
Pod (both head and workers) generates its own private key file (`tls.key`) and self-signed
certificate file (`tls.crt`) in its init container. We generate separate files for each Pod
because worker Pods do not have deterministic DNS names, and we cannot use the same
certificate across different Pods.

In the YAML file, you'll find a ConfigMap named `tls` that contains two shell scripts:
`gencert_head.sh` and `gencert_worker.sh`. These scripts are used to generate the private key
and self-signed certificate files (`tls.key` and `tls.crt`) for the Ray head and worker Pods.
An alternative approach for users is to prebake the shell scripts directly into the docker image that's utilized
by the init containers, rather than relying on a ConfigMap.

Please find below a brief explanation of what happens in each of these scripts:
1. A 2048-bit RSA private key is generated and saved as `/etc/ray/tls/tls.key`.
2. A Certificate Signing Request (CSR) is generated using the private key file (`tls.key`)
and the `csr.conf` configuration file.
3. A self-signed certificate (`tls.crt`) is generated using the private key of the
Certificate Authority (`ca.key`) and the previously generated CSR.

The only difference between `gencert_head.sh` and `gencert_worker.sh` is the `[ alt_names ]`
section in `csr.conf` and `cert.conf`. The worker Pods use the fully qualified domain name
(FQDN) of the head Kubernetes Service to establish a connection with the head Pod.
Therefore, the `[alt_names]` section for the head Pod needs to include the FQDN of the head
Kubernetes Service. By the way, the head Pod uses `$POD_IP` to communicate with worker Pods.

```sh
# gencert_head.sh
[alt_names]
DNS.1 = localhost
DNS.2 = $FQ_RAY_IP
IP.1 = 127.0.0.1
IP.2 = $POD_IP

# gencert_worker.sh
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = $POD_IP
```

In [Kubernetes networking model](https://github.com/kubernetes/design-proposals-archive/blob/main/network/networking.md#pod-to-pod), the IP that a Pod sees itself as is the same IP that others see it as. That's why Ray Pods can self-register for the certificates.

# Step 3: Configure environment variables for Ray TLS authentication

To enable TLS authentication in your Ray cluster, set the following environment variables:

- `RAY_USE_TLS`: Either 1 or 0 to use/not-use TLS. If this is set to 1 then all of the environment variables below must be set. Default: 0.
- `RAY_TLS_SERVER_CERT`: Location of a certificate file which is presented to other endpoints so as to achieve mutual authentication (i.e. `tls.crt`).
- `RAY_TLS_SERVER_KEY`: Location of a private key file which is the cryptographic means to prove to other endpoints that you are the authorized user of a given certificate (i.e. `tls.key`).
- `RAY_TLS_CA_CERT`: Location of a CA certificate file which allows TLS to decide whether an endpoint’s certificate has been signed by the correct authority (i.e. `ca.crt`).

For more information on how to configure Ray with TLS authentication, please refer to [Ray's document](https://docs.ray.io/en/latest/ray-core/configure.html#tls-authentication).

# Step 4: Verify TLS authentication

```sh
# Log in to the worker Pod
kubectl exec -it ${WORKER_POD} -- bash

# Since the head Pod has the certificate of $FQ_RAY_IP, the connection to the worker Pods
# will be established successfully, and the exit code of the ray health-check command
# should be 0.
ray health-check --address $FQ_RAY_IP:6379
echo $? # 0

# Since the head Pod has the certificate of $RAY_IP, the connection will fail and an error
# message similar to the following will be displayed: "Peer name raycluster-tls-head-svc is
# not in peer certificate".
ray health-check --address $RAY_IP:6379

# If you add `DNS.3 = $RAY_IP` to the [alt_names] section in `gencert_head.sh`,
# the head Pod will generate the certificate of $RAY_IP.
#
# For KubeRay versions prior to 0.5.0, this step is necessary because Ray workers in earlier
# versions use $RAY_IP to connect with Ray head.
```
