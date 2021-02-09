import { get } from "./requestHandlers";
import { Actor } from "../type/actor";

export const getActors = () => {
  return get<{
    result: boolean;
    message: string;
    data: {
      actors: {
        [actorId: string]: Actor;
      };
    };
  }>("logical/actors");
};
