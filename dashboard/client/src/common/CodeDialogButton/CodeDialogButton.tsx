import { createStyles, Link, makeStyles, Typography } from "@material-ui/core";
import React, { useState } from "react";
import DialogWithTitle from "../DialogWithTitle";

const useStyles = makeStyles((theme) =>
  createStyles({
    configText: {
      whiteSpace: "pre",
    },
  }),
);

export type CodeDialogButtonProps = {
  /**
   * Title of the dialog box
   */
  title: string;
  /**
   * Text for the button. By default it's "View"
   */
  buttonText?: string;
  /**
   * JSON to stringify and show in the dialog.
   */
  json: object;
};

export const CodeDialogButton = ({
  title,
  buttonText = "View",
  json,
}: CodeDialogButtonProps) => {
  const classes = useStyles();

  const [showConfigDialog, setShowConfigDialog] = useState(false);

  const handleConfigClick = () => {
    setShowConfigDialog(true);
  };

  return (
    <React.Fragment>
      <Link component="button" onClick={handleConfigClick}>
        {buttonText}
      </Link>
      {showConfigDialog && (
        <DialogWithTitle
          title={title}
          handleClose={() => {
            setShowConfigDialog(false);
          }}
        >
          <Typography className={classes.configText}>
            {JSON.stringify(json, undefined, 2)}
          </Typography>
        </DialogWithTitle>
      )}
    </React.Fragment>
  );
};
