import axios from "axios";
import { RayConfigRsp } from "../type/config";

export const getRayConfig = () => {
  return axios.get<RayConfigRsp>("ray_config");
};
