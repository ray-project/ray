import { styled } from "@mui/material/styles";
import React, { HTMLAttributes } from "react";

const ButtonSpan = styled("span")(({ theme }) => ({
  color: theme.palette.primary.main,
  "&:hover": {
    cursor: "pointer",
    textDecoration: "underline",
  },
}));

class SpanButton extends React.Component<HTMLAttributes<HTMLSpanElement>> {
  render() {
    const { ...otherProps } = this.props;
    return <ButtonSpan {...otherProps} />;
  }
}

export default SpanButton;
