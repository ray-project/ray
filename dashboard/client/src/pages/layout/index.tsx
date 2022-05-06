import { IconButton, Tooltip } from "@material-ui/core";
import Drawer from "@material-ui/core/Drawer";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import { makeStyles } from "@material-ui/core/styles";
import Typography from "@material-ui/core/Typography";
import { NightsStay, VerticalAlignTop, WbSunny } from "@material-ui/icons";
import classnames from "classnames";
import React, { PropsWithChildren } from "react";
import { RouteComponentProps } from "react-router-dom";

import SpeedTools from "../../components/SpeedTools";
import Logo from "../../logo.svg";

const drawerWidth = 200;

const useStyles = makeStyles((theme) => ({
  root: {
    display: "flex",
    "& a": {
      color: theme.palette.primary.main,
    },
  },
  drawer: {
    width: drawerWidth,
    flexShrink: 0,
    background: theme.palette.background.paper,
  },
  drawerPaper: {
    width: drawerWidth,
    border: "none",
    background: theme.palette.background.paper,
    boxShadow: theme.shadows[1],
  },
  title: {
    padding: theme.spacing(2),
    textAlign: "center",
    lineHeight: "36px",
  },
  divider: {
    background: "rgba(255, 255, 255, .12)",
  },
  menuItem: {
    cursor: "pointer",
    "&:hover": {
      background: theme.palette.primary.main,
    },
  },
  selected: {
    background: `linear-gradient(45deg, ${theme.palette.primary.main} 30%, ${theme.palette.secondary.main} 90%)`,
  },
  child: {
    flex: 1,
  },
}));

const BasicLayout = (
  props: PropsWithChildren<
    { setTheme: (theme: string) => void; theme: string } & RouteComponentProps
  >,
) => {
  const classes = useStyles();
  const { location, history, children, setTheme, theme } = props;

  return (
    <div className={classes.root}>
      <Drawer
        variant="permanent"
        anchor="left"
        className={classes.drawer}
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        <Typography variant="h6" className={classes.title}>
          <img width={48} src={Logo} alt="Ray" /> <br /> Ray Dashboard
        </Typography>
        <List>
          <ListItem
            button
            className={classnames(
              classes.menuItem,
              location.pathname.includes("node") && classes.selected,
            )}
            onClick={() => history.push("/node")}
          >
            <ListItemText>NODES</ListItemText>
          </ListItem>
          <ListItem
            button
            className={classnames(
              classes.menuItem,
              location.pathname.includes("job") && classes.selected,
            )}
            onClick={() => history.push("/job")}
          >
            <ListItemText>JOBS</ListItemText>
          </ListItem>
          <ListItem
            button
            className={classnames(
              classes.menuItem,
              location.pathname.includes("actor") && classes.selected,
            )}
            onClick={() => history.push("/actors")}
          >
            <ListItemText>ACTORS</ListItemText>
          </ListItem>
          <ListItem
            button
            className={classnames(
              classes.menuItem,
              location.pathname.includes("log") && classes.selected,
            )}
            onClick={() => history.push("/log")}
          >
            <ListItemText>LOGS</ListItemText>
          </ListItem>
          <ListItem
            button
            className={classnames(
              classes.menuItem,
              location.pathname.includes("events") && classes.selected,
            )}
            onClick={() => history.push("/events")}
          >
            <ListItemText>EVENTS</ListItemText>
          </ListItem>
          <ListItem
            button
            className={classnames(classes.menuItem)}
            onClick={() => history.push("/")}
          >
            <ListItemText>BACK TO LEGACY DASHBOARD</ListItemText>
          </ListItem>
          <ListItem>
            <IconButton
              color="primary"
              onClick={() => {
                window.scrollTo(0, 0);
              }}
            >
              <Tooltip title="Back To Top">
                <VerticalAlignTop />
              </Tooltip>
            </IconButton>
            <IconButton
              color="primary"
              onClick={() => {
                setTheme(theme === "dark" ? "light" : "dark");
              }}
            >
              <Tooltip title={`Theme - ${theme}`}>
                {theme === "dark" ? <NightsStay /> : <WbSunny />}
              </Tooltip>
            </IconButton>
          </ListItem>
          <SpeedTools />
        </List>
      </Drawer>
      <div className={classes.child}>{children}</div>
    </div>
  );
};

export default BasicLayout;
