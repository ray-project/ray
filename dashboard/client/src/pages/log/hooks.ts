import useSWR from "swr";
import {
  getStateApiDownloadLogUrl,
  getStateApiLog,
  StateApiLogInput,
} from "../../service/log";

export const useStateApiLogs = (
  props: StateApiLogInput,
  path: string | undefined,
) => {
  const downloadUrl = getStateApiDownloadLogUrl(props);

  const {
    data: log,
    isLoading,
    mutate,
  } = useSWR(
    downloadUrl ? ["useDriverLogs", downloadUrl] : null,
    async ([_]) => {
      return getStateApiLog(props);
    },
  );

  return {
    log: isLoading ? "Loading..." : log,
    downloadUrl,
    refresh: mutate,
    path,
  };
};
