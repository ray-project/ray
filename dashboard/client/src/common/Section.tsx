import { Box, BoxProps, Paper, Typography } from "@mui/material";
import { styled } from "@mui/material/styles"
import React, { PropsWithChildren } from "react";
import { ClassNameProps } from "./props";

const ContentContainer = styled(Paper)(({theme}) => ({
  padding: theme.spacing(2),
  height: "100%",
}));

type SectionProps = {
  title?: string;
  noTopPadding?: boolean;
} & ClassNameProps &
  BoxProps;

export const Section = ({
  title,
  children,
  className,
  noTopPadding = false,
  ...props
}: PropsWithChildren<SectionProps>) => {
  return (
    <Box className={className} {...props}>
      {title && (
        <Box paddingBottom={2}>
          <Typography variant="h4">{title}</Typography>
        </Box>
      )}
      <ContentContainer
        variant="outlined"
        sx={[noTopPadding && {paddingTop: 0}]}
      >
        {children}
      </ContentContainer>
    </Box>
  );
};
