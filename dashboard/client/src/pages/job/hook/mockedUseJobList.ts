import { JobStatus } from "../../../type/job";

export const JOB_LIST = [
  {
    job_id: "01000000",
    submission_id: "raysubmit_12345",
    status: JobStatus.PENDING,
  },
  {
    job_id: "02000000",
    submission_id: null,
    status: JobStatus.FAILED,
  },
  {
    job_id: null,
    submission_id: "raysubmit_23456",
    status: JobStatus.RUNNING,
  },
  {
    job_id: "04000000",
    submission_id: "raysubmit_34567",
    status: JobStatus.STOPPED,
  },
  {
    job_id: "05000000",
    submission_id: "raysubmit_45678",
    status: JobStatus.SUCCEEDED,
  },
] as any;
