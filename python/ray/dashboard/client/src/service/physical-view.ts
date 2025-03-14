import { get } from "./requestHandlers";

export type PhysicalViewData = {
  physicalView: {
    [nodeId: string]: {
      resources: {
        [resourceName: string]: {
          total: number;
          available: number;
        };
      };
      actors: {
        [actorId: string]: {
          actorId: string;
          name: string;
          state: string;
          pid?: number;
          requiredResources?: {
            [resourceName: string]: number;
          };
          placementGroup?: {
            id: string;
          };
        };
      };
    };
  };
};

export type PhysicalViewRsp = {
  result: boolean;
  msg: string;
  data: PhysicalViewData;
};

export const getPhysicalViewData = async (
  jobId?: string,
): Promise<PhysicalViewData> => {
  const path = jobId ? `physical_view?job_id=${jobId}` : "physical_view";
  const result = await get<PhysicalViewRsp>(path);
  return result.data.data;
};
