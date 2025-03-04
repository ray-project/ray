import { render, screen } from "@testing-library/react";
import React from "react";
import { STYLE_WRAPPER } from "../../../util/test-utils";
import { useJobProgressByTaskName } from "../hook/useJobProgress";
import { JobTaskNameProgressTable } from "./JobTaskNameProgressTable";

jest.mock("../hook/useJobProgress");

describe("JobTaskNameProgressTable", () => {
  it("renders", async () => {
    (useJobProgressByTaskName as jest.Mock).mockReturnValue({
      progress: [
        {
          name: "task_a",
          progress: {
            numRunning: 5,
            numFailed: 5,
          },
        },
        {
          name: "task_b",
          progress: {
            numRunning: 5,
            numFinished: 2,
          },
        },
      ],
      page: { pageNo: 1, pageSize: 10 },
      setPage: () => {
        // purposefully empty
      },
      total: 2,
    } as any);

    render(
      <STYLE_WRAPPER>
        <JobTaskNameProgressTable jobId="01000000" />
      </STYLE_WRAPPER>,
    );

    await screen.findByText("Task name");
    expect(screen.getByText("task_a")).toBeInTheDocument();
    expect(screen.getByText("task_b")).toBeInTheDocument();
  });
});
