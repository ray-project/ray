import CloseIcon from "@mui/icons-material/Close";
import { Dialog, IconButton, Theme, Typography } from "@mui/material";
import { WithStyles } from "@mui/styles";
import createStyles from "@mui/styles/createStyles";
import withStyles from "@mui/styles/withStyles";
import React, { PropsWithChildren } from "react";

const styles = (theme: Theme) =>
  createStyles({
    paper: {
      padding: theme.spacing(3),
    },
    closeButton: {
      position: "absolute",
      right: theme.spacing(1.5),
      top: theme.spacing(1.5),
      zIndex: 1,
    },
    title: {
      borderBottomColor: theme.palette.divider,
      borderBottomStyle: "solid",
      borderBottomWidth: 1,
      fontSize: "1.5rem",
      lineHeight: 1,
      marginBottom: theme.spacing(3),
      paddingBottom: theme.spacing(3),
    },
  });

type Props = {
  handleClose: () => void;
  title: string;
};

class DialogWithTitle extends React.Component<
  PropsWithChildren<Props> & WithStyles<typeof styles>
> {
  render() {
    const { classes, handleClose, title } = this.props;
    return (
      <Dialog
        classes={{ paper: classes.paper }}
        fullWidth
        maxWidth="md"
        onClose={handleClose}
        open
        scroll="body"
      >
        <IconButton
          className={classes.closeButton}
          onClick={handleClose}
          size="large"
        >
          <CloseIcon />
        </IconButton>
        <Typography className={classes.title}>{title}</Typography>
        {this.props.children}
      </Dialog>
    );
  }
}

export default withStyles(styles)(DialogWithTitle);
