import {
  Button,
  Grid,
  makeStyles,
  MenuItem,
  Paper,
  Select,
} from "@material-ui/core";
import React, { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import LogVirtualView from "../../components/LogView/LogVirtualView";
import TitleCard from "../../components/TitleCard";
import { getJmap, getJstack, getJstat } from "../../service/util";

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(4),
    width: "100%",
  },
  table: {
    marginTop: theme.spacing(4),
    padding: theme.spacing(2),
  },
  pageMeta: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
  },
  search: {
    margin: theme.spacing(1),
  },
}));

const CMDResult = () => {
  const classes = useStyles();
  const { cmd, ip, pid } = useParams() as {
    cmd: string;
    ip: string;
    pid: string;
  };
  const [result, setResult] = useState<string>();
  const [option, setOption] = useState("gcutil");
  const executeJstat = useCallback(
    () =>
      getJstat(ip, pid, option)
        .then((rsp) => {
          if (rsp.data.result) {
            setResult(rsp.data.data.output);
          } else {
            setResult(rsp.data.msg);
          }
        })
        .catch((err) => setResult(err.toString())),
    [ip, pid, option],
  );

  useEffect(() => {
    switch (cmd) {
      case "jstack":
        getJstack(ip, pid)
          .then((rsp) => {
            if (rsp.data.result) {
              setResult(rsp.data.data.output);
            } else {
              setResult(rsp.data.msg);
            }
          })
          .catch((err) => setResult(err.toString()));
        break;
      case "jmap":
        getJmap(ip, pid)
          .then((rsp) => {
            if (rsp.data.result) {
              setResult(rsp.data.data.output);
            } else {
              setResult(rsp.data.msg);
            }
          })
          .catch((err) => setResult(err.toString()));
        break;
      case "jstat":
        executeJstat();
        break;
      default:
        setResult(`Command ${cmd} is not supported.`);
        break;
    }
  }, [cmd, executeJstat, ip, pid]);

  return (
    <div className={classes.root}>
      <TitleCard title={cmd}>
        {cmd === "jstat" && (
          <Paper className={classes.pageMeta}>
            <Grid container spacing={1}>
              <Grid item>
                <Select
                  value={option}
                  onChange={(e) => setOption(e.target.value as string)}
                >
                  {[
                    "class",
                    "compiler",
                    "gc",
                    "gccapacity",
                    "gcmetacapacity",
                    "gcnew",
                    "gcnewcapacity",
                    "gcold",
                    "gcoldcapacity",
                    "gcutil",
                    "gccause",
                    "printcompilation",
                  ].map((e) => (
                    <MenuItem value={e}>{e}</MenuItem>
                  ))}
                </Select>
              </Grid>
              <Grid item>
                <Button onClick={executeJstat}>Execute</Button>
              </Grid>
            </Grid>
          </Paper>
        )}
      </TitleCard>
      <TitleCard title={`IP: ${ip} / Pid: ${pid}`}>
        <LogVirtualView
          content={result || "loading"}
          language="prolog"
          height={800}
        />
      </TitleCard>
    </div>
  );
};

export default CMDResult;
