import {
  Box,
  createStyles,
  makeStyles,
  Paper,
  Typography,
  Grid,
  styled
} from "@material-ui/core";
import React, { useState } from "react";
import { ActorGroup, ActorState } from "../../../api";
import Actor from "./Actor";
import LabeledDatum from '../../../common/LabeledDatum';
import { Expander, Minimizer } from '../../../common/ExpandControls';
import ActorStateRepr from './ActorStateRepr';

const CenteredBox = styled(Box)({
  textAlign: "center",
});

const useActorClassGroupStyles = makeStyles((theme) =>
  createStyles({
    container: {
      margin: theme.spacing(1),
      padding: theme.spacing(1),
      marginLeft: theme.spacing(2),
    },
    title: {
      margin: theme.spacing(1)
    },
    actorEntry: {
      width: "100%",
    },
  }),
);

type ActorClassGroupProps = {
  title: string;
  actorGroup: ActorGroup;
};

const ActorClassGroup: React.FC<ActorClassGroupProps> = ({ actorGroup,
  title }) => {
  const classes = useActorClassGroupStyles();
  const [expanded, setExpanded] = useState(false);
  const toggleExpanded = () => setExpanded(!expanded);
  const entries = actorGroup.entries.map((actor, i) => (
    <Box component="div" className={classes.actorEntry}>
      <Actor actor={actor} key={actor.actorId ?? i} />
    </Box>
  ));
  const { Alive, PendingResources, Infeasible } = ActorState;
  const summary = actorGroup.summary;
  return (
    <Paper className={classes.container}>
        <Box display="block" className={classes.title}>
          <Typography variant="h5">{title}</Typography>
        </Box>
        <Grid container className={classes.title}>
          <LabeledDatum
            label={<ActorStateRepr state={Alive} variant="body1" />}
            datum={Alive in summary.stateToCount ? summary.stateToCount[Alive] : 0}
          />
          <LabeledDatum
            label={<ActorStateRepr state={Infeasible} variant="body1" />}
            datum={Infeasible in summary.stateToCount ? summary.stateToCount[Infeasible] : 0}
          />
          <LabeledDatum
            label={<ActorStateRepr state={PendingResources} variant="body1" />}
            datum={PendingResources in summary.stateToCount ? summary.stateToCount[PendingResources] : 0}
          />
          <LabeledDatum label={"Mean Lifetime"} datum={summary.avgLifetime} />
          <LabeledDatum label={"Max Lifetime"} datum={summary.maxLifetime} />
          <LabeledDatum label={"Executed Tasks"} datum={summary.numExecutedTasks} />
          <LabeledDatum label={"Pending Tasks"} datum={summary.numPendingTasks} />
        </Grid>
      {expanded ?
        <>
          <Box>{entries}</Box>
          <CenteredBox>
            <Minimizer onClick={toggleExpanded} />
          </CenteredBox>
          </>
        : <CenteredBox>
          <Expander onClick={toggleExpanded} />
        </CenteredBox>}
    </Paper>
  );
};

export default ActorClassGroup;
