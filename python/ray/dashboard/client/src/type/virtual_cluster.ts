export type VirtualCluster = {
  name: string;
  divisible: boolean;
  dividedClusters: Record<string, string>;
  replicaSets: Record<string, number>;
  undividedReplicaSets: Record<string, number>;
  resourcesUsage: Record<string, string>;
  resources: {
    CPU: string;
    memory: string;
    object_store_memory?: string;
  };
  visibleNodeInstances?: Record<string, NodeInstance>;
  undividedNodes?: Record<string, NodeInstance>;
};

export type NodeInstance = {
  hostname: string;
  template_id: string;
  is_dead: boolean;
};
