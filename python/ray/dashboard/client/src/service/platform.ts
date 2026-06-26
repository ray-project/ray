import { PlatformEventsRsp } from "../type/platform";
import { get, head } from "./requestHandlers";

export const getPlatformEvents = async () => {
  return await get<PlatformEventsRsp>("api/v0/platform_events");
};

export const getPlatformEventsEnabled = async (): Promise<boolean> => {
  try {
    await head("api/v0/platform_events");
    return true;
  } catch (e: any) {
    return e?.response?.status !== 404;
  }
};
