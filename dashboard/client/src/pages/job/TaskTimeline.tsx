// import { Button, makeStyles } from "@material-ui/core";
// import dayjs from "dayjs";
import React from "react";
// import React, { useState, useEffect } from "react";
// import Loading from "../../components/Loading";
// import { getTaskTimeline } from "../../service/task";

// const PERFETTO_URL = "https://ui.perfetto.dev";

// const useStyle = makeStyles((theme) => ({
//   root: {
//     padding: theme.spacing(2),
//   },
//   taskProgressTable: {
//     marginTop: theme.spacing(2),
//   },
// }));

// let timer: null | any = undefined;

export const TaskTimeline = () => { return <div></div>};
//     jobId = null,
//   }: {
//     jobId?: string | null;
//   }) => {
//   const classes = useStyle();
//   const [isLoadingData, setLoadingData] = useState(false);

//   useEffect(() => {
//     // declare the data fetching function
//     const fetchData = async () => {
//       const resp = await getTaskTimeline(jobId);
//       return resp;
//     }
  
//     fetchData().then((resp) => {
//         openTrace(resp.data);
//       }).catch((err) => {
//         setLoadingData(false);
//         console.error(err);
//       });
//   });

//   const onClick = (job_id: string | null) => {
//     if (isLoadingData || isLoadingPerfetto) {
//         return;
//     }

//     setLoadingData(true);
//     getTaskTimeline(job_id).then((resp) => {
//       openTrace(resp.data);
//     });
//   };
  
//   // Code from https://perfetto.dev/docs/visualization/deep-linking-to-perfetto-ui
//   const openTrace = (arrayBuffer: any) => {
//     const win = window.open(PERFETTO_URL);
//     window.addEventListener("message", (evt) => onMessage(evt, win, arrayBuffer));
//     if (win) {
//       timer = setInterval(() => win.postMessage("PING", PERFETTO_URL), 50);
//     }
//   };
  
//   const onMessage = (evt: any, win: any, arrayBuffer: any) => {
//     if (evt.data !== "PONG") {
//       return;
//     }
//     window.clearInterval(timer);
//     timer = null;
//     win.postMessage(arrayBuffer, PERFETTO_URL);
//   };

//   const ButtonContents = (() => {
//     if (isLoadingData) {
//         return "Loading data";
//     } else if (isLoadingPerfetto) {
//         return "Waiting for Perfetto to respond";
//     } else {
//         return "Timeline";
//     }
//   });

//   return (
//     <div className={classes.root}>
//         <Button onClick={() => onClick(jobId)} color="primary">
//           {ButtonContents}
//         </Button>
//     </div>
//   );
// };

