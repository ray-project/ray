import { styled } from "@mui/material";

export const SpanButton = styled("span")(({ theme }) => ({
  button: {
    color: theme.palette.primary.main,
    "&:hover": {
      cursor: "pointer",
      textDecoration: "underline",
    },
  },
}));
