import { blueGrey, grey, lightBlue } from "@material-ui/core/colors";
import { createMuiTheme, ThemeOptions } from "@material-ui/core/styles";

const basicTheme: ThemeOptions = {
  typography: {
    fontSize: 12,
    fontFamily: [
      "-apple-system",
      "BlinkMacSystemFont",
      '"Segoe UI"',
      "Roboto",
      '"Helvetica Neue"',
      "Arial",
      "sans-serif",
      '"Apple Color Emoji"',
      '"Segoe UI Emoji"',
      '"Segoe UI Symbol"',
    ].join(","),
    body1: {
      fontSize: "0.75rem",
    },
  },
  props: {
    MuiPaper: {
      elevation: 0,
    },
  },
};

export const lightTheme = createMuiTheme({
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

export const darkTheme = createMuiTheme({
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
