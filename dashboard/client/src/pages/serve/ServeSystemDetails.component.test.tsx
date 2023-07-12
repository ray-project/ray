import { render, screen } from "@testing-library/react";
import React from "react";
import { ActorEnum } from "../../type/actor";
import {
  ServeApplicationStatus,
  ServeSystemActorStatus,
} from "../../type/serve";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { ServeSystemPreview } from "./ServeSystemDetails";

jest.mock("../actor/hook/useActorDetail");
const mockedUseFetchActor = jest.mocked(useFetchActor);

describe("ServeSystemDetails", () => {
  it("renders", async () => {
    expect.assertions(7);

    mockedUseFetchActor.mockReturnValue({
      data: {
        state: ActorEnum.ALIVE,
      },
    } as any);

    render(
      <ServeSystemPreview
        allApplications={
          [
            {
              status: ServeApplicationStatus.UNHEALTHY,
            },
            {
              status: ServeApplicationStatus.RUNNING,
            },
            {
              status: ServeApplicationStatus.DEPLOYING,
            },
          ] as any
        }
        httpProxies={
          [
            {
              status: ServeSystemActorStatus.HEALTHY,
            },
            {
              status: ServeSystemActorStatus.STARTING,
            },
          ] as any
        }
        serveDetails={
          {
            controller_info: {
              actor_id: "actor_id",
            },
          } as any
        }
      />,
      { wrapper: TEST_APP_WRAPPER },
    );
    await screen.findByText("STARTING");
    // Controller and HTTP Proxy
    expect(screen.getAllByText("HEALTHY")).toHaveLength(2);
    expect(screen.getByText("STARTING")).toBeInTheDocument();
    // Applications
    expect(screen.getByText("RUNNING")).toBeInTheDocument();
    expect(screen.getByText("DEPLOYING")).toBeInTheDocument();
    expect(screen.getByText("UNHEALTHY")).toBeInTheDocument();

    expect(
      screen.getByText(/View system status and configuration/),
    ).toBeInTheDocument();

    expect(mockedUseFetchActor).toBeCalledWith("actor_id");
  });
});
