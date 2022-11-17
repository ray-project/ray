import { useContext, useState } from "react";
import { RouteComponentProps } from "react-router-dom";
import useSWR from "swr";
import { GlobalContext } from "../../../App";
import { getJobDetail } from "../../../service/job";

export const useJobDetail = (props: RouteComponentProps<{ id: string }>) => {
  const {
    match: { params },
  } = props;
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const { ipLogMap } = useContext(GlobalContext);
  const { data: job } = useSWR(
    "useJobDetail",
    async () => {
      try {
        const rsp = await getJobDetail(params.id);
        return rsp.data;
      } catch (e) {
        setMsg("Job Query Error Please Check JobId");
        setRefresh(false);
      }
    },
    { refreshInterval: refreshing ? 4000 : 0 },
  );

  return {
    job,
    msg,
    params,
    ipLogMap,
  };
};
