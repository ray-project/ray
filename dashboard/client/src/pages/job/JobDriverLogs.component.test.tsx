import { render, screen } from "@testing-library/react";
import React from "react";
import { get } from "../../service/requestHandlers";
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
          submission_id: "raysubmit_12345",
          driver_node_id: "node-id-0",
        }}
      />,
    );

    await screen.findByText(/log line/);
    expect(screen.getByText(/log line/)).toBeVisible();
    expect(screen.getByText(/this is a line/)).toBeVisible();
    expect(screen.getByText(/Hi/)).toBeVisible();
    expect(screen.getByText(/10/)).toBeVisible();
    expect(screen.getByText(/foo/)).toBeVisible();

    expect(mockedGet).toBeCalledWith(
      "api/v0/logs/file?node_id=node-id-0&filename=job-driver-raysubmit_12345.log&lines=-1",
    );
  });
});
