import CloseIcon from "@mui/icons-material/Close";
import { Dialog, IconButton, Theme, Typography } from "@mui/material";
import React, { PropsWithChildren } from "react";

type Props = {
  handleClose: () => void;
  title: string;
};

class DialogWithTitle extends React.Component<PropsWithChildren<Props>> {
  render() {
    const { handleClose, title } = this.props;
    return (
      <Dialog
        fullWidth
        maxWidth="md"
        onClose={handleClose}
        open
        scroll="body"
        PaperProps={{ sx: { padding: 3 } }}
      >
        <IconButton
          sx={(theme: Theme) => ({
            position: "absolute",
            right: theme.spacing(1.5),
            top: theme.spacing(1.5),
            zIndex: 1,
          })}
          onClick={handleClose}
          size="large"
        >
          <CloseIcon />
        </IconButton>
        <Typography
          sx={(theme) => ({
            borderBottomColor: theme.palette.divider,
            borderBottomStyle: "solid",
            borderBottomWidth: 1,
            fontSize: "1.5rem",
            lineHeight: 1,
            marginBottom: 3,
            paddingBottom: 3,
          })}
        >
          {title}
        </Typography>
        {this.props.children}
      </Dialog>
    );
  }
}

export default DialogWithTitle;
