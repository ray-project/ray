---
myst:
  html_meta:
    description: "Authenticate private remote URIs in runtime_env with a netrc file or HTTPS bearer tokens, on VMs and with KubeRay secrets."
---

(runtime-env-auth)=
# Authenticate remote URIs in `runtime_env`

This page describes how to authenticate the remote URIs in your `runtime_env` without leaking credentials. It covers best practices for authentication and how to provide credentials safely in KubeRay.

(authenticating-remote-uris)=

## Keep credentials out of remote URIs

You can add dependencies to your `runtime_env` with [remote URIs](remote-uris). For publicly hosted files, paste the public URI into your `runtime_env`:

```python
runtime_env = {"working_dir": (
        "https://github.com/"
        "username/repo/archive/refs/heads/master.zip"
    )
}
```

Privately hosted dependencies, such as files in a private GitHub repository, require authentication. One common way to authenticate is to insert credentials into the URI itself:

```python
runtime_env = {"working_dir": (
        "https://username:personal_access_token@github.com/"
        "username/repo/archive/refs/heads/master.zip"
    )
}
```

In this example, `personal_access_token` is a secret credential that authenticates this URI. Ray can access your dependencies through authenticated URIs, but don't include secret credentials in your URIs, for the following two reasons:

1. Ray might log the URIs in your `runtime_env`, so the Ray logs could contain your credentials.
1. Ray stores your remote dependency package in a local directory and uses a parsed version of the remote URI, including your credential, as the directory's name.

In short, Ray doesn't treat your remote URI as a secret, so the URI shouldn't contain secret information. Use a `netrc` file instead.

(running-on-vms-the-netrc-file)=

## Use a netrc file on VMs

The [netrc file](https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html) contains credentials that Ray uses to log in to remote servers automatically. Set your credentials in this file instead of in the remote URI:

```bash
# "$HOME/.netrc"

machine github.com
login username
password personal_access_token
```

In this example, the `machine github.com` line specifies the `login` and `password` to use for any access to `github.com`.

:::{note}
On Unix, name the `netrc` file `.netrc`. On Windows, name the file `_netrc`.
:::

The `netrc` file requires owner read and write access, so run the `chmod` command after you create the file:

```bash
chmod 600 "$HOME/.netrc"
```

Add the `netrc` file to your VM container's home directory so Ray can access the private remote URIs in your `runtime_env`, even when they don't contain credentials.

(running-on-kuberay-secrets-with-netrc)=

## Use a netrc secret on KubeRay

[KubeRay](kuberay-index) can also obtain credentials for remote URIs from a `netrc` file. Supply your `netrc` file through a Kubernetes secret and a Kubernetes volume with the following steps:

1\. Launch your Kubernetes cluster.

2\. Create the `netrc` file locally in your home directory.

3\. Store the `netrc` file's contents as a Kubernetes secret on your cluster:

```bash
kubectl create secret generic netrc-secret --from-file=.netrc="$HOME/.netrc"
```

4\. Expose the secret to your KubeRay application with a mounted volume, and set the `NETRC` environment variable to point to the `netrc` file. Include the following YAML in your KubeRay config:

```yaml
headGroupSpec:
    ...
    containers:
        - name: ...
          image: rayproject/ray:2.56.1
          ...
          volumeMounts:
            - mountPath: "/home/ray/netrcvolume/"
              name: netrc-kuberay
              readOnly: true
          env:
            - name: NETRC
              value: "/home/ray/netrcvolume/.netrc"
    volumes:
        - name: netrc-kuberay
          secret:
            secretName: netrc-secret

workerGroupSpecs:
    ...
    containers:
        - name: ...
          image: rayproject/ray:2.56.1
          ...
          volumeMounts:
            - mountPath: "/home/ray/netrcvolume/"
              name: netrc-kuberay
              readOnly: true
          env:
            - name: NETRC
              value: "/home/ray/netrcvolume/.netrc"
    volumes:
        - name: netrc-kuberay
          secret:
            secretName: netrc-secret
```

5\. Apply your KubeRay config.

Your KubeRay application can use the `netrc` file to access private remote URIs, even when they don't contain credentials.

(using-bearer-tokens-for-https-authentication)=

## Use bearer tokens for HTTPS authentication

As an alternative to a `netrc` file, you can authenticate HTTPS remote URIs with bearer tokens. Bearer tokens are useful for APIs that require OAuth 2.0 or similar token-based authentication.

Set the `RAY_RUNTIME_ENV_BEARER_TOKEN` environment variable to your bearer token:

```bash
export RAY_RUNTIME_ENV_BEARER_TOKEN="your_bearer_token_here"
```

Ray automatically includes this token in the `Authorization` header when it downloads HTTPS URIs in your `runtime_env`:

```python
runtime_env = {"working_dir": "https://example.com/private/repo.zip"}
```

Ray sends the bearer token as an `Authorization: Bearer your_bearer_token_here` header with the HTTPS request.

(running-on-kuberay-bearer-tokens-with-secrets)=

### Use a bearer token secret on KubeRay

For KubeRay deployments, provide the bearer token securely through a Kubernetes secret:

1\. Create a Kubernetes secret containing your bearer token:

```bash
kubectl create secret generic bearer-token-secret \
  --from-literal=RAY_RUNTIME_ENV_BEARER_TOKEN="your_bearer_token_here"
```

2\. Expose the secret to your KubeRay application through environment variables. Include the following YAML in your KubeRay config:

```yaml
headGroupSpec:
    ...
    containers:
        - name: ...
          image: rayproject/ray:2.56.1
          ...
          env:
            - name: RAY_RUNTIME_ENV_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: bearer-token-secret
                  key: RAY_RUNTIME_ENV_BEARER_TOKEN

workerGroupSpecs:
    ...
    containers:
        - name: ...
          image: rayproject/ray:2.56.1
          ...
          env:
            - name: RAY_RUNTIME_ENV_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: bearer-token-secret
                  key: RAY_RUNTIME_ENV_BEARER_TOKEN
```

3\. Apply your KubeRay config.

Your KubeRay application uses the bearer token to authenticate HTTPS requests when it downloads remote URIs in the `runtime_env`.

