import { SubmitInfo, SubmitListRsp } from "../type/submit";
import { get } from "./requestHandlers";

export const getSubmitList = () => {
  return get<SubmitListRsp>("api/submit/list");
};

export const getSubmitDetail = (id: string) => {
  return get<SubmitInfo>(`api/submit/${id}`);
};
