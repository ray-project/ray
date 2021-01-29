import axios from "axios";
import { Actor } from "../type/actor";

export const getActors = () => {
  return axios.get<{
    result: boolean;
    message: string;
    data: {
      actors: {
        [actorId: string]: Actor;
      };
    };
  }>("logical/actors");
};
