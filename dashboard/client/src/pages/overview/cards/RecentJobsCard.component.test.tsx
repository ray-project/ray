import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { useJobList } from "../../job/hook/useJobList";
import { RecentJobsCard } from "./RecentJobsCard";

jest.mock("../../job/hook/useJobList");

describe("RecentJobsCard", () => {
  it("renders", async () => {
    const mockedUseJobList = jest.mocked(useJobList);

    mockedUseJobList.mockReturnValue({
      jobList: [
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
      ],
    } as any);

    render(<RecentJobsCard />, { wrapper: MemoryRouter });

    await screen.findByText("01000000");
    expect(screen.getByText("02000000")).toBeVisible();
    expect(screen.getByText("raysubmit_23456")).toBeVisible();
    expect(screen.getByText("04000000")).toBeVisible();
    expect(screen.queryByText("05000000")).toBeNull();
  });
});
