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

    // Success covers the entire bar
    expect(segments[0].style.width).toEqual("100%");
    // Error covers everything except the last 30% that success covers
    expect(segments[1].style.width).toEqual("70%");
    // In progress covers everything except the last 80% that success and error covers
    expect(segments[2].style.width).toEqual("20%");
  });
});
