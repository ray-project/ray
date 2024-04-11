import { DatasetResponse } from "../type/data";
import { get } from "./requestHandlers";

export const getDataDatasets = (jobId: string | null) => {
  return get<DatasetResponse>(`api/data/datasets/${jobId}`);
};
