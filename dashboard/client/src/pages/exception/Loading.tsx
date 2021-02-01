import React from "react";
import Logo from "../../logo.svg";

export default () => {
  return (
    <div style={{ height: "100vh", width: "100vw" }}>
      <div
        style={{
          margin: "250px auto 0 auto",
          textAlign: "center",
          fontSize: 40,
          fontWeight: 500,
        }}
      >
        <img src={Logo} alt="Loading" width={100} />
        <br />
        Loading...
      </div>
    </div>
  );
};
