import { render, screen } from "@testing-library/react";
import React from "react";
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
    );

    await screen.findByText(/error/);
    expect(screen.getByText(/in progress/)).toBeInTheDocument();
    expect(screen.getByText(/success/)).toBeInTheDocument();

    const segments = screen.getAllByTestId("progress-bar-segment");
    expect(segments).toHaveLength(3);

    expect(segments[0].style.flex).toEqual("2");
    expect(segments[1].style.flex).toEqual("5");
    expect(segments[2].style.flex).toEqual("3");
  });
});
