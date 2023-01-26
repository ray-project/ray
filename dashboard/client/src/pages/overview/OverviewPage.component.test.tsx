import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { useJobList } from "../job/hook/useJobList";
import { OverviewPage } from "./OverviewPage";

jest.mock("../job/hook/useJobList");

describe("OverviewPage", () => {
  it("renders", async () => {
    const mockedUseJobList = jest.mocked(useJobList);

    mockedUseJobList.mockReturnValue({
      jobList: [
        {
          job_id: "01000000",
          submission_id: "raysubmit_12345",
          status: "SUCCEEDED",
        },
      ],
    } as any);

    render(<OverviewPage />, { wrapper: MemoryRouter });
    await screen.findByText(/Events/);
    expect(screen.getByText(/Node metrics/)).toBeVisible();
  });
});
