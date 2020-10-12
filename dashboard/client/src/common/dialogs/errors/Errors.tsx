import {
  createStyles,
  makeStyles,
  fade,
  Theme,
  Typography,
} from "@material-ui/core";
import React, { useState, useEffect } from "react";
import { ErrorsByPid, getErrors } from "../../../api";
import DialogWithTitle from "../../DialogWithTitle";
import NumberedLines from "../../NumberedLines";

const useErrorPaneStyles = makeStyles((theme: Theme) =>
  createStyles({
    header: {
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      marginTop: theme.spacing(3),
    },
    error: {
      backgroundColor: fade(theme.palette.error.main, 0.04),
      borderLeftColor: theme.palette.error.main,
      borderLeftStyle: "solid",
      borderLeftWidth: 2,
      marginTop: theme.spacing(3),
      padding: theme.spacing(2),
    },
    timestamp: {
      color: theme.palette.text.secondary,
      marginBottom: theme.spacing(1),
    },
  }));

type FetchingErrorPaneProps = {
  clearErrorDialog: () => void;
  nodeIp: string;
  pid: number | null;
};

export const FetchingErrorPane: React.FC<FetchingErrorPaneProps> = ({
  nodeIp,
  clearErrorDialog,
  pid,
}) => {
  // errors holds the error entries for the entity for whom the pane
  // is displayed, while fetchError holds an error encountered
  // trying to fetch data from the API.
  const [errors, setErrors] = useState<ErrorsByPid | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);
  useEffect(() => {
      (async () => {
        try {
          const result = await getErrors(nodeIp, pid);
          setErrors(result.errors);
          setFetchError(null);
        } catch (error) {
          setErrors(null);
          setFetchError(error.toString());
        }
      })();
  }, [nodeIp, pid]);
  return (
    <ErrorPane
      errors={errors}
      fetchError={fetchError}
      clearErrorDialog={clearErrorDialog}
      groupTag={nodeIp} />
  )
};

type ErrorPaneProps = {
  clearErrorDialog: () => void;
  errors: ErrorsByPid | null;
  fetchError: string | null;
  groupTag: string;
};

export const ErrorPane: React.FC<ErrorPaneProps> = ({
  clearErrorDialog,
  errors,
  fetchError,
  groupTag,
}) => {
  const classes = useErrorPaneStyles();
  return (<DialogWithTitle handleClose={clearErrorDialog} title="Errors">
    {fetchError !== null ? (
      <Typography color="error">{fetchError}</Typography>
    ) : errors === null ? (
      <Typography color="textSecondary">Loading...</Typography>
    ) : (
          Object.entries(errors).map(([pid, errorEntries]) => (
            <React.Fragment key={pid}>
              <Typography className={classes.header}>
                {groupTag} (PID: {pid})
              </Typography>
              {errorEntries.length > 0 ? (
                errorEntries.map(({ message, timestamp }, index) => (
                  <div className={classes.error} key={index}>
                    <Typography className={classes.timestamp}>
                      Error at {new Date(timestamp * 1000).toLocaleString()}
                    </Typography>
                    <NumberedLines lines={message.trim().split("\n")} />
                  </div>
                ))
              ) : (
                  <Typography color="textSecondary">No errors found.</Typography>
                )}
            </React.Fragment>
          ))
        )}
  </DialogWithTitle>)
};

export default FetchingErrorPane;
