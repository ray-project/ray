import React, { Fragment } from "react";
import { ActorState, RayletInfoResponse } from "../../../api";
import Actor from "./Actor";

type ActorProps = {
  actors: RayletInfoResponse["actors"];
};

const Actors = (props: ActorProps) => {
  const { actors } = props;
  const actorChildren = Object.entries(actors)
    .sort(([, actor1], [, actor2]) => {
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
    .map(([aid, actor]) => <Actor actor={actor} key={aid} />);
  return <Fragment>{actorChildren}</Fragment>;
};

export default Actors;
