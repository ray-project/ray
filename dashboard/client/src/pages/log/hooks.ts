import useSWR from "swr";
import { getStateApiDownloadLogUrl, getStateApiLog } from "../../service/log";

export const useStateApiLogs = (
  driver_node_id?: string | null,
  filename?: string,
) => {
  const downloadUrl =
    driver_node_id && filename
      ? getStateApiDownloadLogUrl(driver_node_id, filename)
      : undefined;

  const {
    data: log,
    isLoading,
    mutate,
  } = useSWR(
    driver_node_id && filename
      ? ["useDriverLogs", driver_node_id, filename]
      : null,
    async ([_, node_id, filename]) => {
      return getStateApiLog(node_id, filename);
    },
  );

  return {
    log: isLoading ? "Loading..." : log,
    downloadUrl,
    refresh: mutate,
    path: filename,
  };
};
