import { get } from "./requestHandlers";
import { RayConfigRsp } from "../type/config";

export const getRayConfig = () => {
  return get<RayConfigRsp>("api/ray_config");
};
