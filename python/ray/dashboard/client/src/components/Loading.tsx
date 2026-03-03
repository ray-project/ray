import { CircularProgress } from "@mui/material";
import React from "react";

const Loading = ({ loading }: { loading: boolean }) =>
  loading ? <CircularProgress color="primary" /> : null;

export default Loading;
