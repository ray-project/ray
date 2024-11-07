import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowRightIcon from "@mui/icons-material/KeyboardArrowRight";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import React, { useEffect, useRef, useState } from "react";

export type OverflowCollapsibleCellProps = {
  text: string;
  maxWidth?: number;
  wordBreak?: "break-all" | "break-word";
};

const OverflowCollapsibleCell = ({
  text,
  maxWidth = 100,
  wordBreak = "break-word",
}: OverflowCollapsibleCellProps) => {
  const [open, setOpen] = useState(false);
  const [isOverflow, setIsOverflow] = useState(false);
  const textRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (textRef.current) {
      setIsOverflow(textRef.current.scrollWidth > textRef.current.clientWidth);
    }
  }, [text]);

  return (
    <Tooltip title={text} arrow>
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <Box
          ref={textRef}
          sx={{
            overflow: open ? "auto" : "hidden",
            width: `${maxWidth}px`,
            textOverflow: open ? "clip" : "ellipsis",
            whiteSpace: open ? "normal" : "nowrap",
            wordBreak,
          }}
        >
          {text}
        </Box>
        <IconButton
          aria-label="expand row"
          size="small"
          onClick={() => setOpen(!open)}
          sx={{ display: isOverflow ? "block" : "none" }}
        >
          {open ? <KeyboardArrowDownIcon /> : <KeyboardArrowRightIcon />}
        </IconButton>
      </Box>
    </Tooltip>
  );
};

export default OverflowCollapsibleCell;
