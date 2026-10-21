# Idle Termination

Starting from KubeRay 1.8.0 and Ray 2.60.0, idle termination of a RayCluster is introduced to reclaim unused RayCluster with no dirvers such as RayJob or RayService attached to it. 

It not only scale down the worker nodes but also suspend or delete an entire idle RayCluster, including the head pod. (TODO this might not be precise enough)

This termination behavior comes with two policies. First, is the default policy, 'Suspend'. The suspend policy deletes all pods but keeps the RayCluster CR so that users can resume the CR by manually patching the `spec.idleSuspend: false`. The main benefit is that users get to preserve expensive env var that are time consuming to create. The second policy is `Delete`, which not only clear all pods but also the CR as well. The Ray autoscaler determines if a RayCluster is idled by checking if it has no drivers attached to it for `spec.idleTerminationOptions.timeoutSeconds`.

## Prerequisite

To use idle termination, enable autoscaler v2 with 
`enableInTreeAutoscaling: true` and make sure the ray version is greater or equal to 2.60.0

RBAC?

## Scope of idle termination

Idle Termination only takes effect for RayCluster CR.
It does not work against RayJob and RayServe. 

## When does ray autoscaler mark a RayCluster as idle
The ray autoscaler queries the GCS job table to see if there is no drivers for this cluster for timeout seconds. Noted that detached actors is not counted as a driver so it will not reset the timeout. 

If a driver attaches, the timeout resets.

See XXX for more info about detached actors.

## How to set up this feature

see link to sample yaml

idle termination options 
timeoutSeconds
policy

## more on what each policy does

- Suspend: 
merge-patches spec.idleSuspend true on the RayCluster CR. with idleSuspend set to true, it will follow what spec.suspend: true does, delete the underlying pods but not the CR itself.
- Delete:
Delete the CR. the ray autoscaler first send an json-patch add operation to add a finalizer ray.op/idle-termination-cleanup-finalizer, then the KubeRay operators will log this event and remove the finalizer to proceed the deletion of the RayCluster CR. There is no resuming with this policy.


## Example 

copy from manual testing steps
```
