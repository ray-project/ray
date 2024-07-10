import { NodeDetailRsp, NodeListRsp, NodeResourceFlagRsp } from "../type/node";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export const getNodeResourceFlag = async () => {
  return await get<NodeResourceFlagRsp>("nodes_resource_flag");
};
