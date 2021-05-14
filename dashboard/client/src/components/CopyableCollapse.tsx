import { Tooltip } from "@material-ui/core";
import { CheckOutlined, FileCopyOutlined } from "@material-ui/icons";
import copy from "copy-to-clipboard";
import React, { useState } from "react";

const CopyableCollapse: React.FC<{ text: string; length?: number, noCollapse?: boolean }> = (
  props,
) => {
  const { text, length = 5, noCollapse } = props;
  const isTextLonger = text.length > length && !noCollapse;
  const displayText = isTextLonger ? `${text.slice(0, length)}` : text;
  const [copying, setCopying] = useState(false);
  const Icon = copying ? CheckOutlined : FileCopyOutlined;

  return (
    <Tooltip title={text}>
      <span style={{ display: "flex", alignItems: "center" }}>
        {displayText}{" "}
        <Icon
          style={{ width: 12, height: 12, cursor: "pointer" }}
          onClick={() => {
            copy(text);
            setCopying(true);
            setTimeout(() => setCopying(false), 1000);
          }}
        />
      </span>
    </Tooltip>
  );
};

export default CopyableCollapse;
