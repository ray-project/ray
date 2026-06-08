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

export type PlacementGroup = {
  placement_group_id: string;
  name: string;
  creator_job_id: string;
  state: PlacementGroupState | string;
  stats?: {
    [key: string]: number | string;
  } | null;
  bundles: Bundle[];
  // Topology strategy: map from label key (e.g. "ray.io/gpu-domain") to
  // placement strategy name. protobuf_message_to_dict serializes the enum as
  // its name string, e.g. "STRICT_PACK".
  topology_strategy?: {
    [key: string]: string;
  } | null;
  // Topology assignments: map from topology label key to the value the
  // scheduler selected for this PG (e.g. {"ray.io/gpu-domain": "rack-1"}).
  topology_assignments?: {
    [key: string]: string;
  } | null;
};
