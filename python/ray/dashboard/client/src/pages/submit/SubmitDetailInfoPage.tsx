import { Box, Typography } from "@mui/material";
import React from "react";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "../../common/CodeDialogButton";
import { DurationText } from "../../common/DurationText";
import { formatDateFromTimeMs } from "../../common/formatUtils";
import { SubmitStatusWithIcon } from "../../common/JobStatus";
import {
  CpuProfilingLink,
  CpuStackTraceLink,
  MemoryProfilingButton,
} from "../../common/ProfilingLink";
import { filterRuntimeEnvSystemVariables } from "../../common/util";
import Loading from "../../components/Loading";
import { MetadataSection } from "../../components/MetadataSection";
import { StatusChip } from "../../components/StatusChip";
import TitleCard from "../../components/TitleCard";
import { SubmitInfo } from "../../type/submit";
import { MainNavPageInfo } from "../layout/mainNavContext";

import { useSubmitDetail } from "./hook/useSubmitDetail";

export const SubmitDetailInfoPage = () => {
  // TODO(aguo): Add more content to this page!
  const { submit, msg, isLoading, params } = useSubmitDetail();

  if (!submit) {
    return (
      <Box sx={{ padding: 2, backgroundColor: "white" }}>
        <MainNavPageInfo
          pageInfo={{
            title: "Info",
            id: "job-info",
            path: "info",
          }}
        />
        <Loading loading={isLoading} />
        <TitleCard title={`SUBMIT - ${params.submitId}`}>
          <StatusChip type="submit" status="LOADING" />
          <br />
          Request Status: {msg} <br />
        </TitleCard>
      </Box>
    );
  }

  return (
    <Box sx={{ padding: 2, backgroundColor: "white" }}>
      <MainNavPageInfo
        pageInfo={{
          title: "Info",
          id: "submit-info",
          path: submit.submission_id
            ? `/jobs/${submit.submission_id}/info`
            : undefined,
        }}
      />
      <Typography variant="h2">{submit.submission_id}</Typography>
      <SubmitMetadataSection submit={submit} />
    </Box>
  );
};

type SubmitMetadataSectionProps = {
  submit: SubmitInfo;
};

export const SubmitMetadataSection = ({
  submit,
}: SubmitMetadataSectionProps) => {
  return (
    <MetadataSection
      metadataList={[
        {
          label: "Submission ID",
          content: {
            value: submit.submission_id,
            copyableValue: submit.submission_id,
          },
        },
        {
          label: "Entrypoint",
          content: submit.entrypoint
            ? {
                value: submit.entrypoint,
                copyableValue: submit.entrypoint,
              }
            : { value: "-" },
        },
        {
          label: "Entrypoint CPU",
          content: {
            value: submit.entrypoint_num_cpus.toFixed(2).toString(),
          },
        },
        {
          label: "Entrypoint GPU",
          content: {
            value: submit.entrypoint_num_gpus.toFixed(2).toString(),
          },
        },
        {
          label: "Entrypoint Memory",
          content: {
            value: submit.entrypoint_memory.toFixed(2).toString(),
          },
        },
        {
          label: "Entrypoint Resource",
          content: submit.entrypoint_resources
            ? {
                value: submit.entrypoint_resources.toString(),
              }
            : { value: "-" },
        },
        {
          label: "Status",
          content: (
            <React.Fragment>
              <SubmitStatusWithIcon submit={submit} />{" "}
              {submit.message && (
                <CodeDialogButton
                  title="Status details"
                  code={submit.message}
                  buttonText="View details"
                />
              )}
            </React.Fragment>
          ),
        },
        {
          label: "Duration",
          content: submit.start_time ? (
            <DurationText
              startTime={submit.start_time}
              endTime={submit.end_time}
            />
          ) : (
            <React.Fragment>-</React.Fragment>
          ),
        },
        {
          label: "Started at",
          content: {
            value: submit.start_time
              ? formatDateFromTimeMs(submit.start_time)
              : "-",
          },
        },
        {
          label: "Ended at",
          content: {
            value: submit.end_time
              ? formatDateFromTimeMs(submit.end_time)
              : "-",
          },
        },
        {
          label: "Runtime environment",
          ...(submit.runtime_env
            ? {
                content: (
                  <CodeDialogButton
                    title="Runtime environment"
                    code={filterRuntimeEnvSystemVariables(submit.runtime_env)}
                  />
                ),
              }
            : {
                content: {
                  value: "-",
                },
              }),
        },
        ...(submit.type === "SUBMISSION"
          ? [
              {
                label: "User-provided metadata",
                content:
                  submit.metadata && Object.keys(submit.metadata).length ? (
                    <CodeDialogButtonWithPreview
                      sx={{ display: "inline-flex", maxWidth: "100%" }}
                      title="User-provided metadata"
                      code={JSON.stringify(submit.metadata, undefined, 2)}
                    />
                  ) : undefined,
              },
            ]
          : []),
        {
          label: "Actions",
          content: (
            <div>
              <CpuStackTraceLink
                pid={submit.driver_info?.pid}
                ip={submit.driver_info?.node_ip_address}
                type="Driver"
              />
              <br />
              <CpuProfilingLink
                pid={submit.driver_info?.pid}
                ip={submit.driver_info?.node_ip_address}
                type="Driver"
              />
              <br />
              <MemoryProfilingButton
                pid={submit.driver_info?.pid}
                ip={submit.driver_info?.node_ip_address}
                type="Driver"
              />
            </div>
          ),
        },
      ]}
    />
  );
};
