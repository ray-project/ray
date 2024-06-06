import CloseIcon from "@mui/icons-material/Close";
import { Dialog, IconButton, Typography } from "@mui/material";
import { styled } from "@mui/material/styles";
import React, { PropsWithChildren } from "react";

const PaperDialog = styled(Dialog)(({theme}) => ({
  paper: {
    padding: theme.spacing(3),
  },
}));

const CloseIconButton = styled(IconButton)(({theme}) => ({
  position: "absolute",
  right: theme.spacing(1.5),
  top: theme.spacing(1.5),
  zIndex: 1,
}));

const TitleTypography = styled(Typography)(({theme}) => ({
  borderBottomColor: theme.palette.divider,
  borderBottomStyle: "solid",
  borderBottomWidth: 1,
  fontSize: "1.5rem",
  lineHeight: 1,
  marginBottom: theme.spacing(3),
  paddingBottom: theme.spacing(3),
}));

type Props = {
  handleClose: () => void;
  title: string;
};

class DialogWithTitle extends React.Component<PropsWithChildren<Props>> {
  render() {
    const { handleClose, title } = this.props;
    return (
      <PaperDialog
        fullWidth
        maxWidth="md"
        onClose={handleClose}
        open
        scroll="body"
      >
        <CloseIconButton
          onClick={handleClose}
          size="large"
        >
          <CloseIcon />
        </CloseIconButton>
        <TitleTypography>{title}</TitleTypography>
        {this.props.children}
      </PaperDialog>
    );
  }
}

export default DialogWithTitle;
