export enum PlacementGroupState {
  // Must be in sync with gcs.proto PlacementGroupState
  PENDING = "PENDING",
  CREATED = "CREATED",
  REMOVED = "REMOVED",
  RESCHEDULING = "RESCHEDULING",
}

export type Bundle = {
  bundle_id: string;
  node_id: string | null;
  unit_resources: {
    [key: string]: number;
  };
  label_selector?: {
    [key: string]: string;
  } | null;
};

export type TopologyStrategyLevel = {
  // Map from label key (e.g. "ray.io/gpu-domain") to placement strategy name.
  // protobuf_message_to_dict serializes the enum as its name string,
  // e.g. "STRICT_PACK".
  entries?: {
    [key: string]: string;
  } | null;
};

export type TopologyAssignmentLevel = {
  // Map from topology label key to the value the scheduler selected for this
  // PG (e.g. {"ray.io/gpu-domain": "rack-1"}).
  assignments?: {
    [key: string]: string;
  } | null;
};

export type PlacementGroup = {
  placement_group_id: string;
  name: string;
  creator_job_id: string;
  state: PlacementGroupState | string;
  stats?: {
    [key: string]: number | string;
  } | null;
  bundles: Bundle[];
  // Per-level topology strategy. Each level is a label->strategy map. For v1,
  // at most one level is populated.
  topology_strategy?: TopologyStrategyLevel[] | null;
  // Per-level topology assignments. Each level is a label->selected-value map.
  // For v1, at most one level is populated.
  topology_assignments?: TopologyAssignmentLevel[] | null;
};
