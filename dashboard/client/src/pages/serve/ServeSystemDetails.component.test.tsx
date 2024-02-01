import { render, screen } from "@testing-library/react";
import React from "react";
import { ActorEnum } from "../../type/actor";
import {
  ServeDeploymentStatus,
  ServeSystemActorStatus,
} from "../../type/serve";
import { TEST_APP_WRAPPER } from "../../util/test-utils";
import { useFetchActor } from "../actor/hook/useActorDetail";
import { ServeSystemPreview } from "./ServeSystemDetails";

jest.mock("../actor/hook/useActorDetail");
const mockedUseFetchActor = jest.mocked(useFetchActor);

describe("ServeSystemDetails", () => {
  it("renders", async () => {
    expect.assertions(6);

    mockedUseFetchActor.mockReturnValue({
      data: {
        state: ActorEnum.ALIVE,
      },
    } as any);

    render(
      <ServeSystemPreview
        allDeployments={
          [
            {
              status: ServeDeploymentStatus.HEALTHY,
            },
            {
              status: ServeDeploymentStatus.UNHEALTHY,
            },
            {
              status: ServeDeploymentStatus.UPDATING,
            },
          ] as any
        }
        proxies={
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
    // Controller, Proxy, and Deployment
    expect(screen.getAllByText("HEALTHY")).toHaveLength(3);
    expect(screen.getByText("STARTING")).toBeInTheDocument();
    // Other deployments
    expect(screen.getByText("UPDATING")).toBeInTheDocument();
    expect(screen.getByText("UNHEALTHY")).toBeInTheDocument();

    expect(
      screen.getByText(/View system status and configuration/),
    ).toBeInTheDocument();

    expect(mockedUseFetchActor).toBeCalledWith("actor_id");
  });
});
