import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  createStyles,
  makeStyles,
  Paper,
  Typography,
} from "@material-ui/core";
import ExpandMoreIcon from "@material-ui/icons/ExpandMore";
import React from "react";
import { ActorGroup } from "../../../api";
import Actor from "./Actor";

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
  return (
    <Paper className={classes.container}>
      <Accordion defaultExpanded={true}>
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          aria-controls="panel1a-content"
          id="panel1a-header"
        >
          <Typography variant="h5">{title}</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Box>{entries}</Box>
        </AccordionDetails>
      </Accordion>
    </Paper>
  );
};

export default ActorClassGroup;
