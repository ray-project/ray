import { Backdrop, CircularProgress } from "@material-ui/core";
import React from "react";

const Loading = ({ loading }: { loading: boolean }) => (
  <Backdrop open={loading} style={{ zIndex: 100 }}>
    <CircularProgress color="primary" />
  </Backdrop>
);

export default Loading;
