import {
  NodeDetailRsp,
  NodeListRsp,
  NodeWithWorkersListRsp,
} from "../type/node";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodesWithWorkers = async () => {
  return await get<NodeWithWorkersListRsp>("nodes?view=details");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};
