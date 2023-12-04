import { get } from "./requestHandlers";

export const MAX_LINES_FOR_LOGS = 50_000;

export type StateApiLogInput = {
  nodeId?: string | null;
  /**
   * If actorId is provided, nodeId is not necessary
   */
  actorId?: string | null;
  /**
   * If taskId is provided, nodeId is not necessary
   */
  taskId?: string | null;
  suffix?: string;
  /**
   * If filename is provided, suffix is not necessary
   */
  filename?: string | null;
  /**
   * -1 for all lines.
   */
  maxLines?: number;
};

export const getStateApiDownloadLogUrl = ({
  nodeId,
  filename,
  taskId,
  actorId,
  suffix,
  maxLines = MAX_LINES_FOR_LOGS,
}: StateApiLogInput) => {
  if (
    nodeId === null ||
    actorId === null ||
    taskId === null ||
    filename === null
  ) {
    // Null means data is not ready yet.
    return null;
  }
  const variables = [
    ...(nodeId !== undefined ? [`node_id=${encodeURIComponent(nodeId)}`] : []),
    ...(filename !== undefined
      ? [`filename=${encodeURIComponent(filename)}`]
      : []),
    ...(taskId !== undefined ? [`task_id=${encodeURIComponent(taskId)}`] : []),
    ...(actorId !== undefined
      ? [`actor_id=${encodeURIComponent(actorId)}`]
      : []),
    ...(suffix !== undefined ? [`suffix=${encodeURIComponent(suffix)}`] : []),
    `lines=${maxLines}`,
  ];

  return `api/v0/logs/file?${variables.join("&")}`;
};

export const getStateApiLog = async (props: StateApiLogInput) => {
  const url = getStateApiDownloadLogUrl(props);
  if (url === null) {
    return undefined;
  }
  const resp = await get<string>(url);
  // Handle case where log file is empty.
  if (resp.status === 200 && resp.data.length === 0) {
    return "";
  }
  // TODO(aguo): get rid of this first byte check once we support state-api logs without this streaming byte.
  if (resp.data[0] !== "1") {
    throw new Error(resp.data.substring(1));
  }
  return resp.data.substring(1);
};

type ListStateApiLogsResponse = {
  data: {
    result: {
      // The response is a list of file names. File names with "/" at the end are directories.
      [logGroup: string]: string[];
    };
  };
};

export const listStateApiLogs = ({
  glob,
  ...props
}: (
  | {
      nodeId: string;
    }
  | {
      nodeIp: string;
    }
) & { glob?: string }) => {
  const nodeId = "nodeId" in props ? props.nodeId : undefined;
  const nodeIp = "nodeIp" in props ? props.nodeIp : undefined;

  const variables = [
    ...(nodeId !== undefined ? [`node_id=${encodeURIComponent(nodeId)}`] : []),
    ...(nodeIp !== undefined ? [`node_ip=${encodeURIComponent(nodeIp)}`] : []),
    ...(glob !== undefined ? [`glob=${encodeURIComponent(glob)}`] : []),
  ];

  const url = `api/v0/logs?${variables.join("&")}`;
  return get<ListStateApiLogsResponse>(url);
};
