import { ClusterStatusMapRsp, NodeDetailRsp, NodeListRsp } from "../type/node";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export const getClusterStatusMap = async () => {
  return await get<ClusterStatusMapRsp>("api/cluster_status");
};
