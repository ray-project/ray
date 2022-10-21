import { useCallback, useContext, useEffect, useRef, useState } from "react";
import { RouteComponentProps } from "react-router-dom";
import { GlobalContext } from "../../../App";
import { getJobDetail } from "../../../service/job";
import { UnifiedJob } from "../../../type/job";

export const useJobDetail = (props: RouteComponentProps<{ id: string }>) => {
  const {
    match: { params },
  } = props;
  const [job, setJob] = useState<UnifiedJob>();
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const { ipLogMap } = useContext(GlobalContext);
  const tot = useRef<NodeJS.Timeout>();
  const getJob = useCallback(async () => {
    if (!refreshing) {
      return;
    }

    try {
      const rsp = await getJobDetail(params.id);
      if (rsp.data) {
        setJob(rsp.data);
      }
    } catch (e) {
      setMsg("Job Query Error Please Check JobId");
      setJob(undefined);
      setRefresh(false);
    }

    tot.current = setTimeout(getJob, 4000);
  }, [refreshing, params.id]);

  useEffect(() => {
    if (tot.current) {
      clearTimeout(tot.current);
    }
    getJob();
    return () => {
      if (tot.current) {
        clearTimeout(tot.current);
      }
    };
  }, [getJob]);

  return {
    job,
    msg,
    params,
    ipLogMap,
  };
};
