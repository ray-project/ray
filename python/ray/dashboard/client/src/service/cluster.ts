import { RayConfigRsp } from "../type/config";
import { get } from "./requestHandlers";

export const getRayConfig = () => {
  return get<RayConfigRsp>("api/ray_config");
};
