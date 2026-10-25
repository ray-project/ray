import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { get } from "../../service/requestHandlers";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { _resetRuntimeEnvRedactionCache } from "../RuntimeEnvRedaction";
import {
  CodeDialogButton,
  CodeDialogButtonWithPreview,
} from "./CodeDialogButton";

jest.mock("../../service/requestHandlers");

const mockedGet = jest.mocked(get);

const mockRedactionEnabled = (redactionEnabled: boolean) => {
  mockedGet.mockResolvedValue({
    data: { data: { redactionEnabled } },
  } as any);
};

beforeEach(() => {
  mockedGet.mockReset();
  mockRedactionEnabled(false);
  _resetRuntimeEnvRedactionCache();
});

describe("CodeDialogButton", () => {
  it("renders with code as JSON", async () => {
    expect.assertions(4);

    render(
      <CodeDialogButton title="Test title" code={{ foo: 1, bar: "bar" }} />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("View");
    expect(screen.getByText("View")).toBeVisible();
    await user.click(screen.getByText("View"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getByText(/foo: 1/)).toBeVisible();
    expect(screen.getByText(/bar: bar/)).toBeVisible();
  });

  it("renders with custom button text and code as a string", async () => {
    expect.assertions(4);

    render(
      <CodeDialogButton
        title="Test title"
        buttonText="CustomButton"
        code="import ray\nray.init()"
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("CustomButton");
    expect(screen.getByText("CustomButton")).toBeVisible();
    await user.click(screen.getByText("CustomButton"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getByText(/import ray/)).toBeVisible();
    expect(screen.getByText(/ray.init\(\)/)).toBeVisible();
  });
});

describe("runtime env redaction help", () => {
  // A redacted runtime env reaches this dialog from the job, Serve, actor and
  // task pages, so the help belongs here rather than on any one page.
  const REDACTED_CONFIG = {
    ray_actor_options: {
      runtime_env: { env_vars: { DB_PASSWORD: "<redacted>" } },
    },
  };

  const openDialog = async (code: object) => {
    render(<CodeDialogButton title="Deployment config" code={code} />, {
      wrapper: TEST_APP_WRAPPER,
    });
    const user = userEvent.setup();
    await screen.findByText("View");
    await user.click(screen.getByText("View"));
    await screen.findByText("Deployment config");
  };

  it("explains the placeholder inside the dialog when values are redacted", async () => {
    mockRedactionEnabled(true);

    await openDialog(REDACTED_CONFIG);

    expect(
      await screen.findByText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
    ).toBeVisible();
    // And points at the CLI as the way to see real values.
    expect(screen.getByText(/ray list runtime-envs/)).toBeVisible();
  });

  it("shows no help when redaction is disabled", async () => {
    mockRedactionEnabled(false);

    await openDialog(REDACTED_CONFIG);

    expect(
      screen.queryByText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
    ).not.toBeInTheDocument();
  });

  it("shows no help when the code has nothing redacted", async () => {
    mockRedactionEnabled(true);

    await openDialog({ ray_actor_options: { num_cpus: 1 } });

    expect(
      screen.queryByText(/RAY_DASHBOARD_REDACT_RUNTIME_ENV=0/),
    ).not.toBeInTheDocument();
  });
});

describe("CodeDialogButtonWithPreview", () => {
  it("renders", async () => {
    expect.assertions(5);

    render(
      <CodeDialogButtonWithPreview
        title="Test title"
        code={{ foo: 1, bar: "bar" }}
      />,
      { wrapper: TEST_APP_WRAPPER },
    );

    const user = userEvent.setup();

    await screen.findByText("Expand");
    // Preview of the code should be visible
    expect(screen.getByText(/foo: 1/)).toBeVisible();
    expect(screen.getByText("Expand")).toBeVisible();
    await user.click(screen.getByText("Expand"));

    await screen.findByText("Test title");
    expect(screen.getByText("Test title")).toBeVisible();
    expect(screen.getAllByText(/foo: 1/)[1]).toBeVisible();
    expect(screen.getAllByText(/bar: bar/)[1]).toBeVisible();
  });
});
