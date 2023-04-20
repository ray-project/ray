import { makeStyles } from "@material-ui/core";
import React, { useContext } from "react";
import { Link } from "react-router-dom";
import { GlobalContext } from "../../App";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { generateNodeLink } from "../../common/links";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
} from "../../common/ProfilingLink";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { MainNavPageInfo } from "../layout/mainNavContext";
import TaskList from "../state/task";
import { useActorDetail } from "./hook/useActorDetail";

const useStyle = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
  },
  paper: {
    padding: theme.spacing(2),
    marginTop: theme.spacing(2),
    marginBottom: theme.spacing(2),
  },
  label: {
    fontWeight: "bold",
  },
  tab: {
    marginBottom: theme.spacing(2),
  },
}));

const ActorDetailPage = () => {
  const classes = useStyle();
  const { ipLogMap } = useContext(GlobalContext);
  const { params, actorDetail, msg, isLoading } = useActorDetail();

  if (!actorDetail) {
    return (
      <div className={classes.root}>
        <Loading loading={isLoading} />
        <TitleCard title={`JOB - ${params.id}`}>
          <StatusChip type="job" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </div>
    );
  }

  return (
    <div className={classes.root}>
      <MainNavPageInfo
        pageInfo={{
          title: `${params.id}`,
          pageTitle: `${params.id} | Actor`,
          id: "actor-detail",
          path: `/actors/${params.id}`,
        }}
      />
      <TitleCard title={`ACTOR - ${params.id}`}>
        <MetadataSection
          metadataList={[
            {
              label: "State",
              content: <StatusChip type="actor" status={actorDetail.state} />,
            },
            {
              label: "ID",
              content: actorDetail.actorId
                ? {
                    value: actorDetail.actorId,
                    copyableValue: actorDetail.actorId,
                  }
                : { value: "-" },
            },
            {
              label: "Name",
              content: actorDetail.name
                ? {
                    value: actorDetail.name,
                  }
                : { value: "-" },
            },
            {
              label: "Class Name",
              content: actorDetail.actorClass
                ? {
                    value: actorDetail.actorClass,
                  }
                : { value: "-" },
            },
            {
              label: "Repr",
              content: actorDetail.reprName
                ? {
                    value: actorDetail.reprName,
                  }
                : { value: "-" },
            },
            {
              label: "Job ID",
              content: actorDetail.jobId
                ? {
                    value: actorDetail.jobId,
                    copyableValue: actorDetail.jobId,
                  }
                : { value: "-" },
            },
            {
              label: "Node ID",
              content: actorDetail.address?.rayletId
                ? {
                    value: actorDetail.address?.rayletId,
                    copyableValue: actorDetail.address?.rayletId,
                    link: actorDetail.address.rayletId
                      ? generateNodeLink(actorDetail.address.rayletId)
                      : undefined,
                  }
                : { value: "-" },
            },
            {
              label: "Worker ID",
              content: actorDetail.address?.workerId
                ? {
                    value: actorDetail.address?.workerId,
                    copyableValue: actorDetail.address?.workerId,
                  }
                : { value: "-" },
            },
            {
              label: "Started at",
              content: {
                value: actorDetail.startTime
                  ? formatDateFromTimeMs(actorDetail.startTime)
                  : "-",
              },
            },
            {
              label: "Ended at",
              content: {
                value: actorDetail.endTime
                  ? formatDateFromTimeMs(actorDetail.endTime)
                  : "-",
              },
            },
            {
              label: "Uptime",
              content: actorDetail.startTime ? (
                <DurationText
                  startTime={actorDetail.startTime}
                  endTime={actorDetail.endTime}
                />
              ) : (
                <React.Fragment>-</React.Fragment>
              ),
            },
            {
              label: "Restarted",
              content: { value: actorDetail.numRestarts },
            },
            {
              label: "Exit Detail",
              content: actorDetail.exitDetail
                ? {
                    value: actorDetail.exitDetail,
                  }
                : { value: "-" },
            },
            {
              label: "Actions",
              content: (
                <div>
                  <Link
                    target="_blank"
                    to={`/logs/${encodeURIComponent(
                      ipLogMap[actorDetail.address?.ipAddress],
                    )}?fileName=${actorDetail.jobId}-${actorDetail.pid}`}
                  >
                    Log
                  </Link>
                  <br />
                  <CpuProfilingLink
                    pid={actorDetail.pid}
                    ip={actorDetail.address?.ipAddress}
                    type=""
                  />
                  <br />
                  <CpuStackTraceLink
                    pid={actorDetail.pid}
                    ip={actorDetail.address?.ipAddress}
                    type=""
                  />
                </div>
              ),
            },
          ]}
        />
      </TitleCard>
      <TitleCard title="Tasks History">
        <TaskList jobId={actorDetail.jobId} actorId={params.id} />
      </TitleCard>
    </div>
  );
};

export default ActorDetailPage;
