import { render, screen } from "@testing-library/react";
import React from "react";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { ProgressBar } from "./ProgressBar";

describe("ProgressBar", () => {
  it("renders", async () => {
    render(
      <ProgressBar
        progress={[
          {
            color: "blue",
            label: "in progress",
            value: 2,
          },
          {
            color: "red",
            label: "error",
            value: 5,
          },
          {
            color: "green",
            label: "success",
            value: 3,
          },
        ]}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText(/error/);
    expect(screen.getByText(/in progress/)).toBeInTheDocument();
    expect(screen.getByText(/success/)).toBeInTheDocument();

    const segments = screen.getAllByTestId("progress-bar-segment");
    expect(segments).toHaveLength(3);
  });

  it("explains unaccounted tasks as excluded from the current view", async () => {
    render(
      <ProgressBar
        progress={[
          {
            color: "green",
            label: "Finished",
            value: 2,
          },
        ]}
        total={3}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    await screen.findByText("Unaccounted: 1");
    expect(
      screen.getByLabelText(
        "Unaccounted tasks are excluded from the current view, such as finished tasks when Hide finished is selected.",
      ),
    ).toBeInTheDocument();
  });
});
