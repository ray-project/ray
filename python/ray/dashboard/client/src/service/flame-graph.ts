import { get } from "./requestHandlers";

export type FlameGraphNode = {
  name: string;
  value: number;
  count?: number;
  totalInParent?: Array<{
    callerNodeId: string;
    duration: number;
    count: number;
  }>;
  actorName?: string;
};

export type FlameGraphData = {
  nodes: Array<{
    id: string;
    nodeId: string;
    startTime: number;
    endTime: number;
    duration: number;
    callerClass: string | null;
    callerFunc: string;
    actorName: string | null;
    actorState?: string;
    parentId?: string;
  }>;
  aggregated: FlameGraphNode[];
};

export type FlameGraphResponse = {
  result: boolean;
  msg: string;
  data: {
    flameData: FlameGraphData;
    jobId: string;
  };
};

export const getFlameGraphData = async (
  jobId?: string,
): Promise<FlameGraphResponse> => {
  try {
    console.log("Fetching flame graph data for jobId:", jobId);
    const response = await get<FlameGraphResponse>(
      `flame_graph${jobId ? `?job_id=${jobId}` : ""}`,
    );
    console.log("Flame graph API response:", response.data);
    return response.data;
  } catch (error) {
    console.error("Error in getFlameGraphData:", error);
    throw error;
  }
};
