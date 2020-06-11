import React, { Fragment } from "react";
import { RayletInfoResponse } from "../../../api";
import Actor from "./Actor";

type ActorProps = {
  actors: RayletInfoResponse["actors"];
};

const Actors = (props: ActorProps) => {
  const { actors } = props;

  const actorChildren = Object.entries(actors).map(([actorId, actor]) => (
    <Actor actor={actor} key={actorId} />
  ));
  return <Fragment>{actorChildren}</Fragment>;
};

export default Actors;
