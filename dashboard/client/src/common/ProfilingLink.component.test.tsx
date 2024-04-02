import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import "@testing-library/jest-dom";
import { TEST_APP_WRAPPER } from "../util/test-utils";
import { ProfilerButton } from "./ProfilingLink";

describe("ProfilerButton", () => {
  const mockProps = {
    profilerUrl: "http://localhost:3000/worker/memory_profile",
  };
  it("renders button correctly", () => {
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);
    expect(button).toBeInTheDocument();
  });

  it("opens the dialog when the button is clicked", async () => {
    const user = userEvent.setup();
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);

    user.click(button);

    // check all components exist in dialog
    await waitFor(() => {
      const dialogTitle = screen.getByText("Memory Profiling Config");
      expect(dialogTitle).toBeInTheDocument();
      const reportButton = screen.getByText(/Generate report/);
      expect(reportButton).toBeInTheDocument();
      const durationInput = screen.getByLabelText(/Duration/);
      expect(durationInput).toBeInTheDocument();
      const leaksCheckbox = screen.getByText(/Leaks/);
      expect(leaksCheckbox).toBeInTheDocument();
      const nativeCheckbox = screen.getByText(/Native/);
      expect(nativeCheckbox).toBeInTheDocument();
      const allocatorCheckbox = screen.getByText(/Python Allocator Tracing/);
      expect(allocatorCheckbox).toBeInTheDocument();
    });
  });

  it("closes the dialog when the cancel button is clicked", async () => {
    const user = userEvent.setup();
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);

    await user.click(button);

    const cancelButton = screen.getByRole("button", { name: /Cancel/ });
    await user.click(cancelButton);

    await waitFor(() => {
      const dialogTitle = screen.queryByText(/Memory Profiling Config/);
      expect(dialogTitle).not.toBeInTheDocument();
    });
  });

  it('selects "flamegraph" as the default format', async () => {
    const user = userEvent.setup();
    render(<ProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    const button = screen.getByLabelText(/Memory Profiling/);
    await user.click(button);

    const formatSelect = screen.getByLabelText(/flamegraph/);
    expect(formatSelect).toBeInTheDocument();
    expect(screen.getByText(/Generate report/)).toHaveAttribute(
      "href",
      `${mockProps.profilerUrl}&format=flamegraph&duration=5&leaks=1&native=0&trace_python_alocators=0`,
    );
  });
});
