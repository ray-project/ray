import axios from "axios";
import { RayConfigRsp } from "../type/config";

export const getRayConfig = () => {
  return axios.get<RayConfigRsp>("api/ray_config");
};
