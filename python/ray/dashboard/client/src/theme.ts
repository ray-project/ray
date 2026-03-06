import { blueGrey, grey, lightBlue } from "@mui/material/colors";
import { createTheme, ThemeOptions } from "@mui/material/styles";

const basicTheme: ThemeOptions = {
  typography: {
    fontSize: 12,
    fontFamily: [
      "Roboto",
      "-apple-system",
      "BlinkMacSystemFont",
      '"Segoe UI"',
      '"Helvetica Neue"',
      "Arial",
      "sans-serif",
      '"Apple Color Emoji"',
      '"Segoe UI Emoji"',
      '"Segoe UI Symbol"',
    ].join(","),
    h1: {
      fontSize: "1.5rem",
      fontWeight: 500,
    },
    h2: {
      fontSize: "1.25rem",
      fontWeight: 500,
    },
    h3: {
      fontSize: "1rem",
      fontWeight: 500,
    },
    h4: {
      fontSize: "1rem",
    },
    body1: {
      fontSize: "0.75rem",
    },
    body2: {
      fontSize: "14px",
      lineHeight: "20px",
    },
    caption: {
      fontSize: "0.75rem",
      lineHeight: "16px",
    },
  },
  components: {
    MuiAutocomplete: {
      defaultProps: {
        size: "small",
      },
    },
    MuiButton: {
      defaultProps: {
        size: "small",
      },
    },
    MuiTextField: {
      defaultProps: {
        size: "small",
      },
    },
    MuiTooltip: {
      styleOverrides: {
        tooltip: {
          fontSize: "0.75rem",
          fontWeight: 400,
          boxShadow: "0px 3px 14px 2px rgba(3, 28, 74, 0.12)",
          padding: 8,
        },
        tooltipPlacementRight: {
          margin: "0 8px",
          "@media (min-width: 600px)": {
            margin: "0 8px",
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        outlined: {
          borderRadius: 8,
        },
      },
    },
  },
};

export const lightTheme = createTheme(basicTheme, {
  palette: {
    mode: "light",
    primary: {
      main: "#036DCF",
    },
    secondary: lightBlue,
    success: {
      main: "#43A047",
    },
    error: {
      main: "#D32F2F",
    },
    warning: {
      main: "#ED6C02",
      light: "#cfcf08",
    },
    text: {
      primary: grey[900],
      secondary: grey[800],
      disabled: grey[400],
    },
    background: {
      paper: "#fff",
      default: blueGrey[50],
    },
    divider: "#D2DCE6",
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        a: {
          color: "#036DCF",
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        outlined: {
          borderColor: "#D2DCE6",
        },
      },
    },
  },
});

export const darkTheme = createTheme(basicTheme, {
  palette: {
    mode: "dark",
    primary: {
      main: "#5B9BFF",
    },
    secondary: lightBlue,
    success: {
      main: "#66BB6A",
    },
    error: {
      main: "#EF5350",
    },
    warning: {
      main: "#FF8C1A",
      light: "#E8E850",
    },
    text: {
      primary: "#E8EAED",
      secondary: "#9AA0A6",
      disabled: "#8A8E93",
    },
    background: {
      paper: "#1A1A1A",
      default: "#0F0F0F",
    },
    divider: "rgba(255, 255, 255, 0.12)",
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        a: {
          color: "#5B9BFF",
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        outlined: {
          borderColor: "rgba(255, 255, 255, 0.12)",
        },
      },
    },
  },
});
