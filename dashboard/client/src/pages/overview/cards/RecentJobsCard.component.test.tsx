import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
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
    render(<RecentJobsCard />, { wrapper: MemoryRouter });

    await screen.findByText("01000000");
    expect(screen.getByText("02000000")).toBeVisible();
    expect(screen.getByText("raysubmit_23456")).toBeVisible();
    expect(screen.getByText("04000000")).toBeVisible();
    expect(screen.getByText("05000000")).toBeVisible();
    expect(screen.getByText("06000000")).toBeVisible();
    expect(screen.queryByText("07000000")).toBeNull();
  });

  it("the link is active when job_id is not null", async () => {
    render(<RecentJobsCard />, { wrapper: MemoryRouter });

    await screen.findByText("01000000");

    const link = screen.getByRole("link", { name: "05000000" });
    expect(link).toHaveAttribute("href");
  });

  it("link is active for driverless job(only have submission_id)", async () => {
    render(<RecentJobsCard />, { wrapper: MemoryRouter });

    await screen.findByText("01000000");

    expect(
      screen.queryByRole("link", { name: "raysubmit_23456" }),
    ).toBeVisible();
  });
});
