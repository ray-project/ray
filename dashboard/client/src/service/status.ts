import { get } from "./requestHandlers";

export type RayStatusResp = {
  result: boolean;
  message: string;
  data: {
    clusterStatus: string;
  };
};

export const getRayStatus = () => {
  return get<RayStatusResp>("api/cluster_status?format=1");
};
