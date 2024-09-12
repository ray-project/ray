import { render, screen } from "@testing-library/react";
import React from "react";
import { TEST_APP_WRAPPER } from "../../../util/test-utils";
import { useJobList } from "../../job/hook/useJobList";
import { RecentJobsCard } from "./RecentJobsCard";

const JOB_LIST = [
  {
    job_id: "01000000",
    submission_id: "raysubmit_12345",
    status: "SUCCEEDED",
  },
  {
    job_id: "02000000",
    submission_id: null,
    status: "FAILED",
  },
  {
    job_id: null,
    submission_id: "raysubmit_23456",
    status: "STOPPED",
  },
  {
    job_id: "04000000",
    submission_id: "raysubmit_34567",
    status: "SUCCEEDED",
  },
  {
    job_id: "05000000",
    submission_id: "raysubmit_45678",
    status: "RUNNING",
  },
  {
    job_id: "06000000",
    submission_id: "raysubmit_56789",
    status: "RUNNING",
  },
  {
    job_id: "07000000",
    submission_id: "raysubmit_67890",
    status: "RUNNING",
  },
];
const mockedUseJobList = jest.mocked(useJobList);

jest.mock("../../job/hook/useJobList");
describe("RecentJobsCard", () => {
  beforeEach(() => {
    mockedUseJobList.mockReturnValue({
      jobList: JOB_LIST,
    } as any);
  });

  it("renders", async () => {
    render(<RecentJobsCard />, { wrapper: TEST_APP_WRAPPER });

    await screen.findByText("raysubmit_12345 (01000000)");
    expect(screen.getByText("02000000")).toBeVisible();
    expect(screen.getByText("raysubmit_23456")).toBeVisible();
    expect(screen.getByText("raysubmit_34567 (04000000)")).toBeVisible();
    expect(screen.getByText("raysubmit_45678 (05000000)")).toBeVisible();
    expect(screen.getByText("raysubmit_56789 (06000000)")).toBeVisible();
    expect(screen.queryByText(/raysubmit_67890/)).toBeNull();
  });

  it("the link is active when job_id is not null", async () => {
    render(<RecentJobsCard />, { wrapper: TEST_APP_WRAPPER });

    await screen.findByText(/raysubmit_12345/);

    const link = screen.getByRole("link", { name: /raysubmit_45678/ });
    expect(link).toHaveAttribute("href");
  });

  it("link is active for driverless job(only have submission_id)", async () => {
    render(<RecentJobsCard />, { wrapper: TEST_APP_WRAPPER });

    await screen.findByText(/raysubmit_12345/);

    expect(
      screen.queryByRole("link", { name: /raysubmit_23456/ }),
    ).toBeVisible();
  });
});
