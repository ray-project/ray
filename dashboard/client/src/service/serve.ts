import { ServeApplicationsRsp } from "../type/serve";
import { get } from "./requestHandlers";

export const getServeApplications = () => {
  return get<ServeApplicationsRsp>("api/serve_head/applications/");
};
