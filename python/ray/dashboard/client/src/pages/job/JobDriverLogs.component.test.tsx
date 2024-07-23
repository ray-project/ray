import { render, screen } from "@testing-library/react";
import React from "react";
import { MAX_LINES_FOR_LOGS } from "../../service/log";
import { get } from "../../service/requestHandlers";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { JobDriverLogs } from "./JobDriverLogs";

jest.mock("../../service/requestHandlers");

const mockedGet = jest.mocked(get);

describe("JobDriverLogs", () => {
  it("renders", async () => {
    expect.assertions(6);

    mockedGet.mockResolvedValue({
      headers: {
        "content-type": "text/plain",
      },
      data: "1log line\nthis is a line\nHi\n10\nfoo",
    });

    render(
      <JobDriverLogs
        job={{
          type: "SUBMISSION",
          job_id: "01000000",
          driver_agent_http_address: "127.0.0.1",
          driver_info: {
            id: "01000000",
            node_id: "node-id-0",
            node_ip_address: "127.0.0.1",
            pid: "12345",
          },
          submission_id: "raysubmit_12345",
          driver_node_id: "node-id-0",
        }}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText(/log line/);
    expect(screen.getByText(/log line/)).toBeVisible();
    expect(screen.getByText(/this is a line/)).toBeVisible();
    expect(screen.getByText(/Hi/)).toBeVisible();
    expect(screen.getByText(/10/)).toBeVisible();
    expect(screen.getByText(/foo/)).toBeVisible();

    expect(mockedGet).toBeCalledWith(
      `api/v0/logs/file?node_id=node-id-0&filename=job-driver-raysubmit_12345.log&lines=${MAX_LINES_FOR_LOGS}&format=leading_1`,
    );
  });
});
