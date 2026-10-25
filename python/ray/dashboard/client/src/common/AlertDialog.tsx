import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
} from "@mui/material";
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

type AlertDialogProps = PropsWithChildren<
  {
    open: boolean;
    handleClose: any;
    onAgree: any;
    title: string;
    contents: string;
  } & ClassNameProps
>;

export const AlertDialog = ({
  open,
  handleClose,
  onAgree,
  title,
  contents,
}: AlertDialogProps) => {
  return (
    <div>
      <Dialog
        open={open}
        onClose={handleClose}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title">{title}</DialogTitle>
        <DialogContent>
          <DialogContentText id="alert-dialog-description">
            {contents}
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleClose}>Disagree</Button>
          <Button onClick={onAgree} autoFocus>
            Agree
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
};
