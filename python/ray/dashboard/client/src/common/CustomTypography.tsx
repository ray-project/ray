import { styled, Typography } from "@mui/material";

export const RightPaddedTypography = styled(Typography)(({ theme }) => ({
  paddingRight: theme.spacing(1),
}));
