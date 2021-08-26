import {
  Grow,
  makeStyles,
  Paper,
  Tab,
  Tabs,
  TextField,
} from "@material-ui/core";
import { red } from "@material-ui/core/colors";
import { Build, Close } from "@material-ui/icons";
import React, { useState } from "react";
import { StatusChip } from "./StatusChip";

const chunkArray = (myArray: string[], chunk_size: number) => {
  const results = [];

  while (myArray.length) {
    results.push(myArray.splice(0, chunk_size));
  }

  return results;
};

const revertBit = (str: string) => {
  return chunkArray(str.split(""), 2)
    .reverse()
    .map((e) => e.join(""))
    .join("");
};

const detectFlag = (str: string, offset: number) => {
  const flag = parseInt(str, 16);
  const mask = 1 << offset;

  return Number(!!(flag & mask));
};

const useStyle = makeStyles((theme) => ({
  toolContainer: {
    background: theme.palette.primary.main,
    width: 48,
    height: 48,
    borderRadius: 48,
    position: "fixed",
    bottom: 100,
    left: 50,
    color: theme.palette.primary.contrastText,
  },
  icon: {
    position: "absolute",
    left: 12,
    cursor: "pointer",
    top: 12,
  },
  popover: {
    position: "absolute",
    left: 50,
    bottom: 48,
    width: 500,
    height: 300,
    padding: 6,
    border: "1px solid",
    borderColor: theme.palette.text.disabled,
  },
  close: {
    float: "right",
    color: theme.palette.error.main,
    cursor: "pointer",
  },
}));

const ObjectIdReader = () => {
  const [id, setId] = useState("");
  const tagList = [
    ["Create From Task", 15, 1],
    ["Put Object", 14, 0],
    ["Return Object", 14, 1],
  ] as [string, number, number][];

  return (
    <div style={{ padding: 8 }}>
      <TextField
        style={{ width: "100%" }}
        id="standard-basic"
        label="Object Id"
        InputProps={{
          onChange: ({ target: { value } }) => {
            setId(value);
          },
        }}
      />
      <div>
        {id.length === 40 ? (
          <div style={{ padding: 8 }}>
            Job ID: {id.slice(24, 28)} <br />
            Actor ID: {id.slice(16, 28)} <br />
            Task ID: {id.slice(0, 28)} <br />
            Index: {parseInt(revertBit(id.slice(32)), 16)} <br />
            Flag: {revertBit(id.slice(28, 32))}
            <br />
            <br />
            {tagList
              .filter(
                ([a, b, c]) => detectFlag(revertBit(id.slice(28, 32)), b) === c,
              )
              .map(([name]) => (
                <StatusChip key={name} type="tag" status={name} />
              ))}
          </div>
        ) : (
          <span style={{ color: red[500] }}>
            Object ID should be 40 letters long
          </span>
        )}
      </div>
    </div>
  );
};

const Tools = () => {
  const [sel, setSel] = useState("oid_converter");
  const toolMap = {
    oid_converter: <ObjectIdReader />,
  } as { [key: string]: JSX.Element };

  return (
    <div>
      <Tabs value={sel} onChange={(e, val) => setSel(val)}>
        <Tab
          value="oid_converter"
          label={<span style={{ fontSize: 12 }}>Object ID Reader</span>}
        />
      </Tabs>
      {toolMap[sel]}
    </div>
  );
};

const SpeedTools = () => {
  const [show, setShow] = useState(false);
  const classes = useStyle();

  return (
    <Paper className={classes.toolContainer}>
      <Build className={classes.icon} onClick={() => setShow(!show)} />
      <Grow in={show} style={{ transformOrigin: "300 500 0" }}>
        <Paper className={classes.popover}>
          <Close className={classes.close} onClick={() => setShow(false)} />
          <Tools />
        </Paper>
      </Grow>
    </Paper>
  );
};

export default SpeedTools;
