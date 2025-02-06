import { ActorDetail } from "../type/actor";
import { get } from "./requestHandlers";

export const getActors = () => {
  return get<{
    result: boolean;
    message: string;
    data: {
      actors: {
        [actorId: string]: ActorDetail;
      };
    };
  }>("logical/actors");
};

export type ActorResp = {
  result: boolean;
  msg: string;
  data: {
    detail: ActorDetail;
  };
};

export const getActor = (actorId: string) => {
  return get<ActorResp>(`logical/actors/${actorId}`);
};
