import { useState } from "react";
import { useParams } from "react-router-dom";
import useSWR from "swr";
import { API_REFRESH_INTERVAL_MS } from "../../../common/constants";
import { getSubmitDetail } from "../../../service/submit";

export const useSubmitDetail = () => {
  const params = useParams() as { submitId: string };
  const [msg, setMsg] = useState("Loading the job detail");
  const [refreshing, setRefresh] = useState(true);
  const { data: submit, isLoading } = useSWR(
    ["useSubmitDetail", params.submitId],
    async ([_, submitId]) => {
      try {
        const rsp = await getSubmitDetail(submitId);
        return rsp.data;
      } catch (e) {
        setMsg("Submit Query Error Please Check submitId");
        setRefresh(false);
      }
    },
    { refreshInterval: refreshing ? API_REFRESH_INTERVAL_MS : 0 },
  );

  return {
    submit,
    isLoading,
    msg,
    params,
  };
};
