import { render, screen } from "@testing-library/react";
import React from "react";
import { OverviewPage } from "./OverviewPage";

describe("OverviewPage", () => {
  it("renders", async () => {
    render(<OverviewPage />);
    await screen.findByText(/Events/);
    expect(screen.getByText(/Node metrics/)).toBeInTheDocument();
  });
});
