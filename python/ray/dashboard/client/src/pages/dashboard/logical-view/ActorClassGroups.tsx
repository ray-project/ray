import React from "react";
import { ActorInfo, ActorGroup } from "../../../api";
import ActorClassGroup from "./ActorClassGroup";

type ActorClassGroupsProps = {
  actorGroups: {[groupKey: string]: ActorGroup};
};

const ActorClassGroups: React.FC<ActorClassGroupsProps> = ({ actorGroups }) => {
  const children = Object.entries(actorGroups)
    .sort(([title], [title2]) => (title > title2 ? 1 : -1))
    .map(([title, actorGroup]) => (
      <ActorClassGroup actorGroup={actorGroup} title={title} key={`acg-${title}`} />
    ));
  return <React.Fragment>{children}</React.Fragment>;
};

export default ActorClassGroups;
