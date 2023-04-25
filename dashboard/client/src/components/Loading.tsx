import { CircularProgress } from "@material-ui/core";
import React from "react";

const Loading = ({ loading }: { loading: boolean }) =>
  loading ? <CircularProgress color="primary" /> : null;

export default Loading;
