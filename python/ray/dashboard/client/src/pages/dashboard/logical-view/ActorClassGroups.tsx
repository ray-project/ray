import React from 'react';
import { ActorInfo } from '../../../api';
import ActorClassGroup from './ActorClassGroup';

type ActorClassGroupsProps = {
  actors: ActorInfo[];
}

const extractClassName = (actor: ActorInfo) => {
  // Given a python class name like Foo(arg1, arg2)
  // this function returns "Foo"
  const re = /(.+)\(/ 
  const matches = actor.actorTitle?.match(re);
  if (matches) {
    return matches[1]
  };
};

const ActorClassGroups: React.FC<ActorClassGroupsProps> = ({ actors }) => {
  const groups = actors.reduce((groups, actor) => {
    const className = extractClassName(actor) ?? "Unknown Class";
    return groups.has(className)
      ? groups.get(className).append(actor)
      : groups.set(className, [actor])
  }, new Map());
  const children = Array.from(groups).map(([title, actors]) =>
    <ActorClassGroup title={title} actors={actors} key={`acg-${title}`} />);
  return (<>
    {children}
      </>)
};

export default ActorClassGroups;