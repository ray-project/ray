import React, { Fragment } from "react";
import { ActorState, RayletInfoResponse } from "../../../api";
import Actor from "./Actor";

type ActorProps = {
  actors: RayletInfoResponse["actors"];
};

const Actors = (props: ActorProps) => {
  const { actors } = props;
  const actorChildren = Object.values(actors)
    .sort((actor1, actor2) => {
      if (
        actor1.state === ActorState.Dead &&
        actor2.state === ActorState.Dead
      ) {
        return 0;
      } else if (actor2.state === ActorState.Dead) {
        return -1;
      } else {
        return 1;
      }
    })
    .map((actor) => <Actor actor={actor} key={actor.actorId} />);
  return <Fragment>{actorChildren}</Fragment>;
};

export default Actors;
