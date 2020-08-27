import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  createStyles,
  makeStyles,
  Paper,
  Typography,
  Grid
} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import React from "react";
import { ActorGroup, ActorState } from "../../../api";
import Actor from "./Actor";
import LabeledDatum from '../../../common/LabeledDatum';
import ActorStateRepr from './ActorStateRepr';

const useActorClassGroupStyles = makeStyles((theme) =>
  createStyles({
    container: {
      margin: theme.spacing(1),
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
  const entries = actorGroup.entries.map((actor, i) => (
    <Box component="div" className={classes.actorEntry}>
      <Actor actor={actor} key={actor.actorId ?? i} />
    </Box>
  ));
  const { Alive, PendingResources, Infeasible } = ActorState;
  const summary = actorGroup.summary;
  return (
    <Paper className={classes.container}>
      <Accordion defaultExpanded={true}>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          aria-controls="panel1a-content"
          id="panel1a-header"
        >
          <Typography variant="h5">{title}</Typography>
          <Grid container className={classes.container}>
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
        </AccordionSummary>
        <AccordionDetails>
          <Box>{entries}</Box>
        </AccordionDetails>
      </Accordion>
    </Paper>
  );
};

export default ActorClassGroup;
