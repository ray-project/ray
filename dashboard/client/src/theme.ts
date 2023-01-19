import { blueGrey, grey, lightBlue } from "@material-ui/core/colors";
import { createTheme, ThemeOptions } from "@material-ui/core/styles";

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
      fontSize: "2rem",
    },
    h2: {
      fontSize: "1.5rem",
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
  props: {
    MuiPaper: {
      elevation: 0,
    },
  },
  overrides: {
    MuiTooltip: {
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
    MuiPaper: {
      outlined: {
        borderColor: "#D2DCE6",
      },
    },
  },
};

export const lightTheme = createTheme({
  ...basicTheme,
  palette: {
    primary: {
      main: "#538DF9",
    },
    secondary: lightBlue,
    success: {
      main: "#43A047",
    },
    error: {
      main: "#D32F2F",
    },
    text: {
      primary: grey[900],
      secondary: grey[800],
      disabled: grey[400],
      hint: grey[300],
    },
    background: {
      paper: "#fff",
      default: blueGrey[50],
    },
  },
});

export const darkTheme = createTheme({
  ...basicTheme,
  palette: {
    primary: {
      main: "#538DF9",
    },
    secondary: lightBlue,
    success: {
      main: "#43A047",
    },
    error: {
      main: "#D32F2F",
    },
    text: {
      primary: blueGrey[50],
      secondary: blueGrey[100],
      disabled: blueGrey[200],
      hint: blueGrey[300],
    },
    background: {
      paper: grey[800],
      default: grey[900],
    },
  },
});
