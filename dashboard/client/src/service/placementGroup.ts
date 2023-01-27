import { PlacementGroup } from "../type/placementGroup";
import { StateApiResponse } from "../type/stateApi";
import { get } from "./requestHandlers";

export const getPlacementGroup = () => {
  return get<StateApiResponse<PlacementGroup>>(
    "api/v0/placement_groups?detail=1&limit=10000",
  );
};
