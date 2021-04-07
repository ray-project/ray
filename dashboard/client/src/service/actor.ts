import { Actor } from "../type/actor";
import { get } from "./requestHandlers";

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
