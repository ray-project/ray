import { get } from "./requestHandlers";

export const getLogDetail = async (url: string) => {
  if (window.location.pathname !== "/" && url !== "log_index") {
    const pathArr = window.location.pathname.split("/");
    if (pathArr.length > 1) {
      const idx = pathArr.findIndex((e) => e.includes(":"));
      if (idx > -1) {
        const afterArr = pathArr.slice(0, idx);
        afterArr.push(url.replace(/https?:\/\//, ""));
        url = afterArr.join("/");
      }
    }
  }
  const rsp = await get(
    url === "log_index" ? url : `log_proxy?url=${encodeURIComponent(url)}`,
  );
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
