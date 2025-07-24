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
};
