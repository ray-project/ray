import { DatasetResponse } from "../type/data";
import { get } from "./requestHandlers";

export const getDataDatasets = () => {
  return get<DatasetResponse>("api/data/datasets");
};
