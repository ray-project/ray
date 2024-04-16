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
# download istioctl and manifests
export ISTIO_VERSION=1.21.1
curl -L https://istio.io/downloadIstio | sh -
cd istio-1.21.1
export PATH=$PWD/bin:$PATH

# install Istio with:
#   1. 100% trace sampling for demo purpose
#   2. "sanitize_te" disabled for proper grpc interception
#   3. TLS 1.3
istioctl install -y -f - <<EOF
apiVersion: install.istio.io/v1alpha1
kind: IstioOperator
spec:
  meshConfig:
    defaultConfig:
      tracing:
        sampling: 100
      runtimeValues:
        envoy.reloadable_features.sanitize_te: "false"
    meshMTLS:
      minProtocolVersion: TLSV1_3
EOF

# install Istio addmons, including kiali dashboard and jaeger dashboard
kubectl apply -f samples/addons
# enable istio sidecar auto injection
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
    ray.io/headless-worker-svc: raycluster-istio
  name: raycluster-istio-headless-svc
  namespace: default
spec:
  clusterIP: None
  selector:
    ray.io/cluster: raycluster-istio
  publishNotReadyAddresses: true
  ports:
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
EOF
```

Note that this Headless Service manifest MUST list ALL the ports, used by Ray explicitly, including ALL worker ports. See [Configuring Ray](https://docs.ray.io/en/latest/ray-core/configure.html#all-nodes) for more details on ports required by Ray.

## Step 4: Create the RayCluster

The upcoming RayCluster MUST use exactly the same ports listed in the previous Headless Service, including the `max-worker-port`.
In addition, the `node-ip-address` MUST be set to the Pod FQDN of the Headless Service to enable Istio L7 observability.

```bash
kubectl apply -f - <<EOF
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-istio
spec:
  rayVersion: '2.10.0-aarch64'
  headGroupSpec:
    rayStartParams:
      num-cpus: '1'
      node-manager-port: '6380'
      object-manager-port: '6381'
      runtime-env-agent-port: '6382'
      dashboard-agent-grpc-port: '6383'
      dashboard-agent-listen-port: '52365'
      metrics-export-port: '8080'
      max-worker-port: '10012'
      node-ip-address: \$(hostname -I | tr -d ' ' | sed 's/\./-/g').raycluster-istio-headless-svc.default.svc.cluster.local
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.10.0-aarch64
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
        dashboard-agent-listen-port: '52365'
        metrics-export-port: '8080'
        max-worker-port: '10012'
        node-ip-address: \$(hostname -I | tr -d ' ' | sed 's/\./-/g').raycluster-istio-headless-svc.default.svc.cluster.local
      template:
        spec:
          containers:
          - name: ray-worker
            image: rayproject/ray:2.10.0-aarch64
EOF
```

## Step 5: Run Ray application to generate traffic

After the RayCluster is ready, use the following script to generate internal traffic.

```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- python -c "import ray; ray.get([ray.remote(lambda x: print(x)).remote(i) for i in range(10000)])"
```

## Step 6: Verify the auto mTLS and L7 observability

```bash
istioctl dashboard kiali
```

Go to http://localhost:20001/kiali/console/namespaces/default/workloads/raycluster-istio?duration=60&refresh=60000&tab=info

```bash
istioctl dashboard kiali
```

Go to http://localhost:16686/jaeger/search?limit=1000&lookback=1h&maxDuration&minDuration&service=raycluster-istio.default