import { NodeDetailRsp, NodeListRsp } from "../type/node";
import { PlatformEventsRsp } from "../type/platform";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export const getPlatformEvents = async () => {
  return await get<PlatformEventsRsp>(`api/v0/platform_events`);
};
