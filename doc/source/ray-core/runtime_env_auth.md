(runtime-env-auth)=
# Authenticating Remote URIs in runtime_env

This section helps you:

* Avoid leaking remote URI credentials in your `runtime_env`
* Provide credentials safely in KubeRay
* Understand best practices for authenticating your remote URI

## Authenticating Remote URIs

You can add dependencies to your `runtime_env` with [remote URIs](remote-uris). This is straightforward for files hosted publicly, because you simply paste the public URI into your `runtime_env`:

```python
runtime_env = {"working_dir": (
        "https://github.com/"
        "username/repo/archive/refs/heads/master.zip"
    )
}
```

However, dependencies hosted privately, in a private GitHub repo for example, require authentication. One common way to authenticate is to insert credentials into the URI itself:

```python
runtime_env = {"working_dir": (
        "https://username:personal_access_token@github.com/"
        "username/repo/archive/refs/heads/master.zip"
    )
}
```

In this example, `personal_access_token` is a secret credential that authenticates this URI. While Ray can successfully access your dependencies using authenticated URIs, **you should not include secret credentials in your URIs** for two reasons:

1. Ray may log the URIs used in your `runtime_env`, which means the Ray logs could contain your credentials.
2. Ray stores your remote dependency package in a local directory, and it uses a parsed version of the remote URI–including your credential–as the directory's name.

In short, your remote URI is not treated as a secret, so it should not contain secret info. Instead, use a `netrc` file.

## Running on VMs: the netrc File

The [netrc file](https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html) contains credentials that Ray uses to automatically log into remote servers. Set your credentials in this file instead of in the remote URI:

```bash
# "$HOME/.netrc"

machine github.com
login username
password personal_access_token
```

In this example, the `machine github.com` line specifies that any access to `github.com` should be authenticated using the provided `login` and `password`.

:::{note}
On Unix, name the `netrc` file as `.netrc`. On Windows, name the
file as `_netrc`.
:::

The `netrc` file requires owner read/write access, so make sure to run the `chmod` command after creating the file:

```bash
chmod 600 "$HOME/.netrc"
```

Add the `netrc` file to your VM container's home directory, so Ray can access the `runtime_env`'s private remote URIs, even when they don't contain credentials.

## Running on KubeRay: Secrets with netrc

[KubeRay](https://ray-project.github.io/kuberay/) can also obtain credentials from a `netrc` file for remote URIs. Supply your `netrc` file using a Kubernetes secret and a Kubernetes volume with these steps:

1\. Launch your Kubernetes cluster.

2\. Create the `netrc` file locally in your home directory.

3\. Store the `netrc` file's contents as a Kubernetes secret on your cluster:

```bash
kubectl create secret generic netrc-secret --from-file=.netrc="$HOME/.netrc"
```

4\. Expose the secret to your KubeRay application using a mounted volume, and update the `NETRC` environment variable to point to the `netrc` file. Include the following YAML in your KubeRay config.

```yaml
headGroupSpec:
    ...
    containers:
        - name: ...
          image: rayproject/ray:latest
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
          image: rayproject/ray:latest
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
