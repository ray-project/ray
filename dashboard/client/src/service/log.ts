import { get } from "./requestHandlers";

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

export const getStateApiDownloadLogUrl = (nodeId: string, fileName: string) =>
  `api/v0/logs/file?node_id=${nodeId}&filename=${fileName}&lines=-1`;

export const getStateApiLog = async (nodeId: string, fileName: string) => {
  const resp = await get<string>(getStateApiDownloadLogUrl(nodeId, fileName));
  // TODO(aguo): get rid of this first byte check once we support state-api logs without this streaming byte.
  if (resp.data[0] !== "1") {
    throw new Error(resp.data.substring(1));
  }
  return resp.data.substring(1);
};
