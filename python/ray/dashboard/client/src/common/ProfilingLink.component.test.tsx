import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import "@testing-library/jest-dom";
import { TEST_APP_WRAPPER } from "../util/test-utils";
import { CpuProfilerButton, ProfilerButton } from "./ProfilingLink";

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
      `${mockProps.profilerUrl}&format=flamegraph&duration=5&leaks=1&native=0&trace_python_allocators=0`,
    );
  });
});

describe("CpuProfilerButton", () => {
  const mockProps = {
    profilerUrl: "worker/cpu_profile?pid=123&node_id=abc",
  };

  it("renders button correctly", () => {
    render(<CpuProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    expect(screen.getByLabelText(/CPU Profiling/)).toBeInTheDocument();
  });

  it("opens the dialog when the button is clicked", async () => {
    const user = userEvent.setup();
    render(<CpuProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    await user.click(screen.getByLabelText(/CPU Profiling/));

    await waitFor(() => {
      expect(screen.getByText("CPU Profiling Config")).toBeInTheDocument();
      expect(screen.getByLabelText(/Duration/)).toBeInTheDocument();
      expect(screen.getByText(/Native/)).toBeInTheDocument();
      expect(screen.getByText(/GIL Only/)).toBeInTheDocument();
      expect(screen.getByText(/Include Idle/)).toBeInTheDocument();
      expect(screen.getByText(/Non-blocking/)).toBeInTheDocument();
    });
  });

  it("shows viewer buttons for chrometrace format", async () => {
    const user = userEvent.setup();
    render(<CpuProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    await user.click(screen.getByLabelText(/CPU Profiling/));

    // Select chrometrace format
    const formatSelect = screen.getByRole("combobox");
    await user.click(formatSelect);
    await user.click(screen.getByText(/Chrome Trace/));

    await waitFor(() => {
      expect(screen.getByText(/Open.*Speedscope/)).toBeInTheDocument();
      expect(screen.getByText(/Open.*Trace.*Viewer/)).toBeInTheDocument();
      expect(screen.getByText(/Open.*Perfetto/)).toBeInTheDocument();
    });
  });

  it("closes the dialog when the cancel button is clicked", async () => {
    const user = userEvent.setup();
    render(<CpuProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    await user.click(screen.getByLabelText(/CPU Profiling/));

    await user.click(screen.getByRole("button", { name: /Cancel/ }));

    await waitFor(() => {
      expect(
        screen.queryByText("CPU Profiling Config"),
      ).not.toBeInTheDocument();
    });
  });

  it('selects "flamegraph" as the default format', async () => {
    const user = userEvent.setup();
    render(<CpuProfilerButton {...mockProps} />, { wrapper: TEST_APP_WRAPPER });
    await user.click(screen.getByLabelText(/CPU Profiling/));

    const formatSelect = screen.getByLabelText(/flamegraph/);
    expect(formatSelect).toBeInTheDocument();
  });
});
