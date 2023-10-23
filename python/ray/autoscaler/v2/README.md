
Instance: An Instance in the autoscaler is a logical unit of cloud provided node and a ray process: a cloud provided node could be a VM (AWS EC2) or a container (e.g. Kuberary), where the ray process is a ray head/worker process (and other ray processes). See the Instance Lifecycle section for more details.

InstanceManager (IM): The instance manager is a proxy for updating and getting current instances states. The IM interacts with the InstanceStorage to store and retrieve instance states.

InstanceStorage: The instance storage is a versioned storage for storing instance states. The InstanceStorage is the source of truth for instance states. It could be implemented using a database or a distributed key-value store.
 
Instance States Subscribers: Whenever an instance state is updated, the instance storage notifies all the subscribers of the instance state change. The subscribers are:
    1. Reconciler: The reconciler reconciles Instance statuses with the cloud node provider, e.g. when node is asynchronously launched, or cloud node is leaked.
    2. Ray Installer: The ray installer installs ray on the cloud node if ray install is needed after the cloud node is allocated.
    3. Instance Launcher: It launches cloud nodes when the launch request is processed.

 