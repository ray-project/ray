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
      data: "log line\nthis is a line\nHi\n10\nfoo",
    });

    render(
      <JobDriverLogs
        job={{
          driver_agent_http_address: "http://127.0.0.1:52365",
          driver_info: {
            id: "01000000",
            node_id: "node-id-0",
            node_ip_address: "127.0.0.1",
            pid: "1234",
          },
          submission_id: "raysubmit_12345",
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
      "log_proxy?url=http%3A%2F%2F127.0.0.1%3A52365%2Flogs%2Fjob-driver-raysubmit_12345.log",
    );
  });
});
