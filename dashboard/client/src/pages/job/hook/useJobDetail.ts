import { useCallback, useContext, useEffect, useRef, useState } from "react";
import { RouteComponentProps } from "react-router-dom";
import { GlobalContext } from "../../../App";
import { getJobDetail } from "../../../service/job";
import { JobDetail } from "../../../type/job";

export const useJobDetail = (props: RouteComponentProps<{ id: string }>) => {
  const {
    match: { params },
  } = props;
  const [job, setJob] = useState<JobDetail>();
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const [selectedTab, setTab] = useState("info");
  const { ipLogMap } = useContext(GlobalContext);
  const tot = useRef<NodeJS.Timeout>();
  const handleChange = (event: React.ChangeEvent<{}>, newValue: string) => {
    setTab(newValue);
  };
  const handleSwitchChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRefresh(event.target.checked);
  };
  const getJob = useCallback(async () => {
    if (!refreshing) {
      return;
    }
    const rsp = await getJobDetail(params.id);

    if (rsp.data?.data?.detail) {
      setJob(rsp.data.data.detail);
    }

    if (rsp.data?.msg) {
      setMsg(rsp.data.msg || "");
    }

    if (rsp.data.result === false) {
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

  const { jobInfo } = job || {};
  const actorMap = job?.jobActors;

  return {
    actorMap,
    jobInfo,
    job,
    msg,
    selectedTab,
    handleChange,
    handleSwitchChange,
    params,
    refreshing,
    ipLogMap,
  };
};
