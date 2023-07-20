import {
  NodeDetailRsp,
  NodeListRsp,
  NodeLogicalResourceRsp,
} from "../type/node";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export const getNodeLogicalResourceMap = async () => {
  return await get<NodeLogicalResourceRsp>("nodes/logical_resource");
};
