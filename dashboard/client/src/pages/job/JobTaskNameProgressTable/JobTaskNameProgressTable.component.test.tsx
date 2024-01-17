import { ThemeProvider, Theme, StyledEngineProvider } from "@mui/material";
import { render, screen } from "@testing-library/react";
import React from "react";
import { lightTheme } from "../../../theme";
import { useJobProgressByTaskName } from "../hook/useJobProgress";
import { JobTaskNameProgressTable } from "./JobTaskNameProgressTable";


declare module '@mui/styles/defaultTheme' {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface
  interface DefaultTheme extends Theme {}
}


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
      <StyledEngineProvider injectFirst>
        <ThemeProvider theme={lightTheme}>
          <JobTaskNameProgressTable jobId="01000000" />
        </ThemeProvider>
      </StyledEngineProvider>,
    );

    await screen.findByText("Task name");
    expect(screen.getByText("task_a")).toBeInTheDocument();
    expect(screen.getByText("task_b")).toBeInTheDocument();
  });
});
