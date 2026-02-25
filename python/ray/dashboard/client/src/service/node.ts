import { NodeDetailRsp, NodeListRsp } from "../type/node";
import { K8sEventsRsp } from "../type/k8s";
import { get } from "./requestHandlers";

export const getNodeList = async () => {
  return await get<NodeListRsp>("nodes?view=summary");
};

export const getNodeDetail = async (id: string) => {
  return await get<NodeDetailRsp>(`nodes/${id}`);
};

export const getK8sEvents = async (nodeId: string) => {
  // We use the node_id query param to filter by node
  return await get<K8sEventsRsp>(`api/v0/k8s_events?node_id=${nodeId}`);
};
