import React from "react";
import { ActorInfo } from "../../../api";
import ActorClassGroup from "./ActorClassGroup";

type ActorClassGroupsProps = {
  actors: ActorInfo[];
};

const extractClassName = (actor: ActorInfo) => {
  // Given a python class name like Foo(arg1, arg2)
  // this function returns "Foo"
  const re = /(.+)\(/;
  const matches = actor.actorTitle?.match(re);
  if (matches) {
    return matches[1];
  }
};

const ActorClassGroups: React.FC<ActorClassGroupsProps> = ({ actors }) => {
  const groups = new Map();
  actors.forEach((actor) => {
    const className = extractClassName(actor) ?? "Unknown Class";
    const existingGroup = groups.get(className);
    if (existingGroup) {
      existingGroup.push(actor);
    } else {
      groups.set(className, [actor]);
    }
  });

  const children = Array.from(groups)
    .sort(([title], [title2]) => (title > title2 ? 1 : -1))
    .map(([title, actorGroup]) => (
      <ActorClassGroup title={title} actors={actorGroup} key={`acg-${title}`} />
    ));
  return <React.Fragment>{children}</React.Fragment>;
};

export default ActorClassGroups;
