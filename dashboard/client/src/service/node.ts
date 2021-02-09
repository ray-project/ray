import { get } from "./requestHandlers";
import { NodeDetailRsp, NodeListRsp } from "../type/node";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};
