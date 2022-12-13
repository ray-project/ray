import { PlacementGroup } from "../type/placementGroup";
import { get } from "./requestHandlers";

export const getPlacementGroup = () => {
  return get<{
    result: boolean;
    message: string;
    data: {
      [result: string]: {
        [result: string]: Array<PlacementGroup>;
      };
    };
  }>("api/v0/placement_groups?detail=1");
};
