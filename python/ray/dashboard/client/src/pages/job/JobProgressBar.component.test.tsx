import { render, screen } from "@testing-library/react";
import React from "react";
import { JobStatus } from "../../type/job";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { useJobProgress, useJobProgressByLineage } from "./hook/useJobProgress";
import { JobProgressBar } from "./JobProgressBar";

jest.mock("./hook/useJobProgress");

describe("JobProgressBar", () => {
  beforeEach(() => {
    (useJobProgressByLineage as jest.Mock).mockReturnValue({
      progressGroups: [],
      isLoading: false,
      total: undefined,
      totalTasks: undefined,
      totalTaskAttempts: undefined,
      numAfterTruncation: undefined,
      latestFetchTimestamp: 0,
    });
  });

  it("renders a warning when task progress data is truncated", async () => {
    (useJobProgress as jest.Mock).mockReturnValue({
      progress: { numFinished: 10000 },
      isLoading: false,
      driverExists: true,
      totalTasks: 10000,
      totalTaskAttempts: 12000,
      numAfterTruncation: 10000,
      latestFetchTimestamp: 1,
    });

    render(
      <JobProgressBar
        jobId="01000000"
        job={{ status: JobStatus.RUNNING }}
        onClickLink={() => {
          // purposefully empty
        }}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText(/Task progress may be incomplete/);
    expect(
      screen.getByText(/2,000 task entries were truncated/),
    ).toBeInTheDocument();
  });
});
