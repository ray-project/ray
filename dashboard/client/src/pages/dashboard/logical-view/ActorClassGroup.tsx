import {
  Box,
  createStyles,
  Grid,
  makeStyles,
  Paper,
  styled,
  Typography,
} from "@material-ui/core";
import React, { useState } from "react";
import SpanButton from "../../../common/SpanButton";
import { ActorGroup, ActorState, ActorInfo, isFullActorInfo, LogsByPid, ErrorsByPid } from "../../../api";
import { Expander, Minimizer } from "../../../common/ExpandControls";
import { LogPane } from '../../../common/dialogs/logs/Logs';
import { ErrorPane } from '../../../common/dialogs/errors/Errors';
import LabeledDatum from "../../../common/LabeledDatum";
import Actor from "./Actor";
import ActorStateRepr from "./ActorStateRepr";

const asSeconds = (n: number) => `${n}s`;
const CenteredBox = styled(Box)({
  textAlign: "center",
});

const actorLogsByPid = (actors: ActorInfo[]) => {
  const logsByPid: LogsByPid = {};
  for (const actor of actors) {
    if (isFullActorInfo(actor)) {
      if (actor.logs.length > 0) {
        const actorPid = actor.processStats.pid.toString();
        logsByPid[actorPid] = actor.logs;
      }
    }
  }
  return logsByPid;
};

const actorErrorsByPid = (actors: ActorInfo[]) => {
  const errorsByPid: ErrorsByPid = {};
  for (const actor of actors) {
    if (isFullActorInfo(actor)) {
      if (actor.errors.length > 0) {
        const actorPid = actor.processStats.pid.toString();
        errorsByPid[actorPid] = actor.errors;
      }
    }
  }
  return errorsByPid;
}

const useActorClassGroupStyles = makeStyles((theme) =>
  createStyles({
    container: {
      margin: theme.spacing(1),
      padding: theme.spacing(1),
      marginLeft: theme.spacing(2),
    },
    title: {
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

const ActorClassGroup: React.FC<ActorClassGroupProps> = ({
  actorGroup,
  title,
}) => {
  const classes = useActorClassGroupStyles();
  const [expanded, setExpanded] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [showErrors, setShowErrors] = useState(false);
  const toggleExpanded = () => setExpanded(!expanded);
  const entries = actorGroup.entries.map((actor, i) => (
    <Box
      component="div"
      className={classes.actorEntry}
      key={actor.actorId ?? i}
    >
      <Actor actor={actor} />
    </Box>
  ));
  const { Alive, PendingResources, Infeasible } = ActorState;
  const summary = actorGroup.summary;
  return (
    <Paper className={classes.container}>
      <Box display="block" className={classes.title}>
        <Typography variant="h5">{title}</Typography>
        {summary.numLogs > 0 &&
          <SpanButton onClick={() => setShowLogs(true)}>
            View Logs ({summary.numLogs})
          </SpanButton>
        }
        {summary.numErrors > 0 &&
          <SpanButton onClick={() => setShowErrors(true)}>
            View Errors ({summary.numErrors})
          </SpanButton>
        }
        {showLogs && 
          <LogPane
            logs={actorLogsByPid(actorGroup.entries)}
            groupTag={title}
            clearLogDialog={() => setShowLogs(false)}
            error={null}
          />
        }
        {showErrors &&
          <ErrorPane
            errors={actorErrorsByPid(actorGroup.entries)}
            groupTag={title}
            clearErrorDialog={() => setShowErrors(false)}
            fetchError={null}
          />
        }
      </Box>
      <Grid container className={classes.title}>
        <LabeledDatum
          label={
            <ActorStateRepr state={Alive} variant="body1" showTooltip={true} />
          }
          datum={
            Alive in summary.stateToCount ? summary.stateToCount[Alive] : 0
          }
        />
        <LabeledDatum
          label={
            <ActorStateRepr
              state={Infeasible}
              variant="body1"
              showTooltip={true}
            />
          }
          datum={
            Infeasible in summary.stateToCount
              ? summary.stateToCount[Infeasible]
              : 0
          }
        />
        <LabeledDatum
          label={
            <ActorStateRepr
              state={PendingResources}
              variant="body1"
              showTooltip={true}
            />
          }
          datum={
            PendingResources in summary.stateToCount
              ? summary.stateToCount[PendingResources]
              : 0
          }
        />
        <LabeledDatum
          label={"Mean Lifetime"}
          datum={asSeconds(summary.avgLifetime)}
        />
        <LabeledDatum
          label={"Max Lifetime"}
          datum={asSeconds(summary.maxLifetime)}
        />
        <LabeledDatum
          label={"Executed Tasks"}
          datum={summary.numExecutedTasks}
        />
      </Grid>
      {expanded ? (
        <React.Fragment>
          <Box>{entries}</Box>
          <CenteredBox>
            <Minimizer onClick={toggleExpanded} />
          </CenteredBox>
        </React.Fragment>
      ) : (
        <CenteredBox>
          <Expander onClick={toggleExpanded} />
        </CenteredBox>
      )}
    </Paper>
  );
};

export default ActorClassGroup;
