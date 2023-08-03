import { get } from "./requestHandlers";

export const MAX_LINES_FOR_LOGS = 50_000;
const getLogUrl = (url: string) => {
  return url === "log_index" ? url : `log_proxy?url=${encodeURIComponent(url)}`;
};

/**
 * @returns Url where we can fetch log contents. Will return null if the url
 * refers to a log directory and not a log file.
 */
export const getLogDownloadUrl = (url: string) => {
  url = getLogUrl(url);
  if (url === "log_index") {
    return undefined;
  }
  return url;
};

export const getLogDetail = async (url: string) => {
  url = getLogUrl(url);
  const rsp = await get(url);
  if (rsp.headers["content-type"]?.includes("html")) {
    const el = document.createElement("div");
    el.innerHTML = rsp.data;
    const arr = [].map.call(
      el.getElementsByTagName("li"),
      (li: HTMLLIElement) => {
        const a = li.children[0] as HTMLAnchorElement;
        let href = a.href;
        if (
          // Skip remove protocal and host at log index page
          url !== "log_index" &&
          !li.innerText.startsWith("http://") &&
          !li.innerText.startsWith("https://")
        ) {
          // Remove protocol and host
          // (Treat everything after the hostname as a string)
          const protocolAndHost = `${a.protocol}//${a.host}`;
          href = href.substring(protocolAndHost.length);
        }
        return {
          name: li.innerText,
          href,
        } as { [key: string]: string };
      },
    );
    return arr as { [key: string]: string }[];
  }

  return rsp.data as string;
};

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
};

export const getStateApiDownloadLogUrl = ({
  nodeId,
  filename,
  taskId,
  actorId,
  suffix,
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
    `lines=${MAX_LINES_FOR_LOGS}`,
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
