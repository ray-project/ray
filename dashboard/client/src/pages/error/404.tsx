import { Typography } from "@material-ui/core";
import { HelpOutlineOutlined } from "@material-ui/icons";
import React from "react";

const Error404 = () => {
  return (
    <div
      style={{
        display: "flex",
        position: "fixed",
        justifyContent: "center",
        alignItems: "center",
        textAlign: "center",
        width: "100%",
        height: "100%",
      }}
    >
      <div style={{ height: 400 }}>
        <Typography variant="h2">
          <HelpOutlineOutlined fontSize="large" />
        </Typography>
        <Typography variant="h6">404 NOT FOUND</Typography>
        <p>
          We can't provide the page you wanted yet, better try with another path
          next time.
        </p>
      </div>
    </div>
  );
};

export default Error404;
