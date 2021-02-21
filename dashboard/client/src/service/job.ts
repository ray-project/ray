import axios from "axios";
import { JobDetailRsp, JobListRsp } from "../type/job";

export const getJobList = () => {
  return axios.get<JobListRsp>("jobs?view=summary");
};

export const getJobDetail = (id: string) => {
  return axios.get<JobDetailRsp>(`jobs/${id}`);
};
