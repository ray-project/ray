import { IconButton, Tooltip, useTheme as useMuiTheme } from "@mui/material";
import React from "react";
import { RiMoonLine, RiSunLine } from "react-icons/ri";
import { useTheme } from "../contexts/ThemeContext";

export const ThemeToggle: React.FC = () => {
  const { mode, toggleTheme } = useTheme();
  const muiTheme = useMuiTheme();
  const isDark = mode === "dark";

  return (
    <Tooltip title={isDark ? "Switch to light mode" : "Switch to dark mode"}>
      <IconButton
        onClick={toggleTheme}
        sx={{ color: muiTheme.palette.text.secondary }}
        size="large"
      >
        {isDark ? <RiSunLine /> : <RiMoonLine />}
      </IconButton>
    </Tooltip>
  );
};
