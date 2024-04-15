(kuberay-istio)=
# Enable mTLS and L7 observability of traffic in RayCluster with Istio

This guide demonstrates the integration of KubeRay and Istio for enabling mTLS and L7 observability of traffic in a RayCluster on a local Kind cluster.

## Istio

[Istio](https://istio.io/) is an open source service mesh that layers transparently onto existing distributed applications.  Istio’s powerful features provide a uniform and more efficient way to secure, connect, and monitor services. Its powerful control plane brings vital features, including:
* Secure service-to-service communication in a cluster with TLS encryption, strong identity-based authentication and authorization.
* Automatic metrics, logs, and traces for all traffic within a cluster.

See the [Istio documentation](https://istio.io/latest/docs/) to learn more.

## Step 0: Create a Kind cluster

```bash
kind create cluster
```

## Step 1: Install Istio

```bash
export ISTIO_VERSION=1.20.5
curl -L https://istio.io/downloadIstio | sh -
cd istio-1.20.5
export PATH=$PWD/bin:$PATH
istioctl install --set profile=minimal -y
kubectl apply -f samples/addons
kubectl label namespace default istio-injection=enabled
```

See [Istio Getting Started](https://istio.io/latest/docs/setup/getting-started/) for more details on installing Istio.

## Step 2: Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Step 3: Apply a Headless service for the upcoming RayCluster

In order to let Istio learn the L7 information of the traffic in the upcoming RayCluster, we must apply a Headless service for it.

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  labels:
    ray.io/headless-worker-svc: raycluster-mini
  name: raycluster-mini-headless-svc
  namespace: default
spec:
  clusterIP: None
  selector:
    ray.io/cluster: raycluster-mini
  publishNotReadyAddresses: true
  ports:
  - name: node-gcs-port
    port: 6379
    appProtocol: grpc
  - name: node-manager-port
    port: 6380
    appProtocol: grpc
  - name: object-manager-port
    port: 6381
    appProtocol: grpc
  - name: runtime-env-agent-port
    port: 6382
    appProtocol: grpc
  - name: dashboard-agent-grpc-port
    port: 6383
    appProtocol: grpc
  - name: dashboard-agent-listen-port
    port: 52365
    appProtocol: http
  - name: metrics-export-port
    port: 8080
    appProtocol: http
  - name: dashboard-port
    port: 8265
    appProtocol: http
  - name: dashboard-grpc-port
    port: 8266
    appProtocol: grpc
  - name: client-port
    port: 10001
    appProtocol: grpc
  - name: p10002
    port: 10002
    appProtocol: grpc
  - name: p10003
    port: 10003
    appProtocol: grpc
  - name: p10004
    port: 10004
    appProtocol: grpc
  - name: p10005
    port: 10005
    appProtocol: grpc
  - name: p10006
    port: 10006
    appProtocol: grpc
  - name: p10007
    port: 10007
    appProtocol: grpc
  - name: p10008
    port: 10008
    appProtocol: grpc
  - name: p10009
    port: 10009
    appProtocol: grpc
  - name: p10010
    port: 10010
    appProtocol: grpc
  - name: p10011
    port: 10011
    appProtocol: grpc
  - name: p10012
    port: 10012
    appProtocol: grpc
  - name: p10013
    port: 10013
    appProtocol: grpc
  - name: p10014
    port: 10014
    appProtocol: grpc
  - name: p10015
    port: 10015
    appProtocol: grpc
  - name: p10016
    port: 10016
    appProtocol: grpc
EOF
```

Note that this YAML manifest should list ALL the ports used by Ray explicitly. See [Configuring Ray](https://docs.ray.io/en/latest/ray-core/configure.html#all-nodes) for more details on ports required by Ray.

## Step 4: Create the RayCluster

```bash
kubectl apply -f - <<EOF
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-mini
spec:
  rayVersion: '2.10.0-aarch64'
  headGroupSpec:
    rayStartParams:
      num-cpus: '1'
      node-manager-port: '6380'
      object-manager-port: '6381'
      runtime-env-agent-port: '6382'
      dashboard-agent-grpc-port: '6383'
      dashboard-grpc-port: '8266'
      max-worker-port: '10016'
      node-ip-address: '\$(hostname -I | sed "s/\./-/g").raycluster-mini-headless-svc.default.svc.cluster.local'
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.10.0-aarch64
          ports:
          - containerPort: 6379
            name: redis
          - containerPort: 8265
            name: dashboard
          - containerPort: 10001
            name: client
  workerGroupSpecs:
    - replicas: 1
      minReplicas: 1
      maxReplicas: 1
      groupName: small-group
      rayStartParams:
        num-cpus: '1'
        node-manager-port: '6380'
        object-manager-port: '6381'
        runtime-env-agent-port: '6382'
        dashboard-agent-grpc-port: '6383'
        max-worker-port: '10016'
        node-ip-address: '\$(hostname -I | sed "s/\./-/g").raycluster-mini-headless-svc.default.svc.cluster.local'
      template:
        spec:
          containers:
          - name: ray-worker
            image: rayproject/ray:2.10.0-aarch64
EOF
```
