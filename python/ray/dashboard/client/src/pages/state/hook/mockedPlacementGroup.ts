export const bundles = [
  {
    bundle_id: "bundle-1",
    node_id: "node-1",
    unit_resources: {
      cpu: 4,
      memory: 8192,
    },
  },
  {
    bundle_id: "bundle-2",
    node_id: null,
    unit_resources: {
      cpu: 2,
      memory: 4096,
    },
  },
  {
    bundle_id: "bundle-3",
    node_id: "node-2",
    unit_resources: {
      cpu: 8,
      memory: 16384,
    },
  },
];

export const mockData = [
  {
    placement_group_id: "pg-123456789",
    name: "MyPlacementGroup",
    creator_job_id: "job-987654321",
    state: "CREATED",
    stats: null,
    bundles,
  },
  {
    placement_group_id: "pg-123456789",
    name: "MyPlacementGroup",
    creator_job_id: "job-987654321",
    state: "REMOVED",
    stats: null,
    bundles,
  },
  {
    placement_group_id: "pg-123456789",
    name: "MyPlacementGroup",
    creator_job_id: "job-987654321",
    state: "RESCHEDULING",
    stats: null,
    bundles,
  },
  {
    placement_group_id: "pg-123456789",
    name: "MyPlacementGroup",
    creator_job_id: "job-987654321",
    state: "PENDING",
    stats: null,
    bundles,
  },
];
