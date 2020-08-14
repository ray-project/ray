import React from 'react';
import { ActorInfo } from '../../../api';
import Actor from './Actor';
import { Typography } from '@material-ui/core';

type ActorClassGroupProps = {
  title: string;
  actors: ActorInfo[];
};

const ActorClassGroup: React.FC<ActorClassGroupProps> = ({
  actors,
  title
}) => {
  const entries = actors.map((actor, i) => <Actor actor={actor} key={actor.actorId ?? i} />);
  return (
    <div>
      <Typography>{title}</Typography>
      {entries}
    </div>
  );
};

export default ActorClassGroup;