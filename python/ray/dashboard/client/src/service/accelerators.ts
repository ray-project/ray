import { AcceleratorRsp } from "../type/accelerator";
import { get } from "./requestHandlers";

export const getAccelerators = async () => {
  return await get<AcceleratorRsp>("/accelerator/get_accelerators");
};
