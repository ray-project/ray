# Ray-Operator Documentation

To introduce the Ray-Operator, give an explanation of RayCluster CR firstly.  
If interested in CRD, refer to file deploy/ray-operator/config/crd/bases/ray.io_rayclusters.yaml for more detail.  
If interested in RayCluster types, refer to file deploy/ray-operator/api/v1/raycluster_types.go.  

## RayCluster sample CR
4 pods in this sample, 1 for head and 3 for workers but with different specifications.
```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  labels:
    controller-tools.k8s.io: "1.0"
  name: raycluster-sample
spec:
  clusterName: raycluster-sample
  images:
    defaultImage: "image"
  imagePullPolicy: "Always"

  extensions:
    - size: 2
      groupName: group1
      type: worker
      idList: ["raycluster-sample-group1-worker-0","raycluster-sample-group1-worker-1"]

      # custom labels. NOTE: do not define custom labels start with `raycluster.`, they may be used in controller.
      labels:
        raycluster.group.name: group1

      # resource requirements
      resources:
        limits:
          cpu: 1000m
          memory: 2Gi
          ephemeral-storage: 2Gi
        requests:
          cpu: 1000m
          memory: 2Gi
          ephemeral-storage: 2Gi

      # environment variables to set in the container
      containerEnv:
        - name: CLUSTER_NAME
          value: raycluster-sample
        - name: POD_READY_FILEPATH
          value: /path/to/log
        - name: MY_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: MY_POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP

      # head service suffix: {namespace}.svc , follows Kubernetes standard
      headServiceSuffix: "ray-operator.svc"

      volumes:
        - name: log-volume
          emptyDir: {}

      volumeMounts:
        - mountPath: /path/to/log
          name: log-volume

    - size: 1
      groupName: group2
      type: worker
      idList: ["raycluster-sample-group2-worker-0"]

      # custom labels. NOTE: do not define custom labels start with `raycluster.`, they may be used in controller.
      labels:
        raycluster.group.name: group2

      # resource requirements
      resources:
        limits:
          cpu: 2000m
          memory: 4Gi
          ephemeral-storage: 4Gi
        requests:
          cpu: 2000m
          memory: 4Gi
          ephemeral-storage: 4Gi

      # environment variables to set in the container
      containerEnv:
        - name: CLUSTER_NAME
          value: raycluster-sample
        - name: POD_READY_FILEPATH
          value: /path/to/log
        - name: MY_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: MY_POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP

      # head service suffix
      headServiceSuffix: "ray-operator.svc"

      volumes:
        - name: log-volume
          emptyDir: {}

      volumeMounts:
        - mountPath: /path/to/log
          name: log-volume

    - size: 1
      groupName: headgroup1
      type: head
      idList: ["raycluster-sample-group2-head-0"]

      # custom labels. NOTE: do not define custom labels start with `raycluster.`, they may be used in controller.
      labels:
        raycluster.group.name: headgroup1

      # resource requirements
      resources:
        limits:
          cpu: 2000m
          memory: 4Gi
          ephemeral-storage: 4Gi
        requests:
          cpu: 2000m
          memory: 4Gi
          ephemeral-storage: 4Gi

      # environment variables to set in the container
      containerEnv:
        - name: CLUSTER_NAME
          value: raycluster-sample
        - name: POD_READY_FILEPATH
          value: /path/to/log
        - name: MY_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: MY_POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP

      # head service suffix
      headServiceSuffix: "ray-operator.svc"

      volumes:
        - name: log-volume
          emptyDir: {}

      volumeMounts:
        - mountPath: /path/to/log
          name: log-volume

```

### apiVersion
* ray.io/v1 - version for RayCluster v1

### kind
* RayCluster - CustomResourceDefinition of Kubernetes, we applied this

### metadata
* labels - key-value pair, metadata
* name -  metadata for the object
    
### spec - specification of the RayCluster        
* clusterName - to distinguish among the multiple clusters  

* images - the image for the pod
  * defaultImage - the default image for pod.  

* imagePullPolicy - the pull policy for pod

* extensions - array of pod group

```
size         // the size of this pod group

type         // worker/head

image        // pod image

groupName    // logic groupName for worker in same group

idList       // the concrete pod name list, separate by ","

labels       // labels for pod

nodeSelector // NodeSelector specifies a map of key-value pairs. For the pod to be eligible
             // to run on a node, the node must have each of the indicated key-value pairs as
             // labels.

affinity     // the affinity for pod

resources    // the resource requirements for this group pod.

tolerations  // Tolerations specifies the pod's tolerations.

containerEnv // List of environment variables to set in the container.

headServiceSuffix // Head service suffix, format {namespace}.svc , follows Kubernetes standard. So head can be accessed by domain name

annotations  // Annotations for pod

volumes      // Volume for the pod group

volumeMounts // VolumeMount for the pod group
```

The attributes above follows the Kubernetes standard, so the format can be accepted.
