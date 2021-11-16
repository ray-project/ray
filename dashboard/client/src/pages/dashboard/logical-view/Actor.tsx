import { createStyles, makeStyles, Theme, Typography } from "@material-ui/core";
import React, { useState } from "react";
import {
  ActorInfo,
  ActorState,
  checkProfilingStatus,
  CheckProfilingStatusResponse,
  getProfilingResultURL,
  isFullActorInfo,
  launchKillActor,
  launchProfiling,
} from "../../../api";
import ActorDetailsPane from "./ActorDetailsPane";

const useActorStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      borderColor: theme.palette.divider,
      borderStyle: "solid",
      borderWidth: 1,
      marginTop: theme.spacing(2),
      padding: theme.spacing(2),
      width: "100%",
    },
    title: {
      color: theme.palette.text.secondary,
      fontSize: "0.75rem",
    },
    action: {
      color: theme.palette.primary.main,
      textDecoration: "none",
      "&:hover": {
        cursor: "pointer",
      },
    },
    infeasible: {
      color: theme.palette.error.main,
    },
    pendingResources: {
      color: theme.palette.secondary.main,
    },

    webuiDisplay: {
      fontSize: "0.875rem",
    },
    inlineHTML: {
      fontSize: "0.875rem",
      display: "inline",
    },
  }),
);

type ActorProps = {
  actor: ActorInfo;
};

type profilingInfo = {
  [profilingId: string]: {
    startTime: number;
    latestResponse: CheckProfilingStatusResponse | null;
  };
};

const Actor: React.FC<ActorProps> = ({ actor }) => {
  const [profiling, setProfiling] = useState<profilingInfo>({});
  const classes = useActorStyles();

  const handleProfilingClick = (duration: number) => async () => {
    if (actor.state === ActorState.Alive) {
      const profilingId = await launchProfiling(
        actor.nodeId,
        actor.pid,
        duration,
      );
      setProfiling({
        ...profiling,
        [profilingId]: { startTime: Date.now(), latestResponse: null },
      });
      const checkProfilingStatusLoop = async () => {
        const response = await checkProfilingStatus(profilingId);
        setProfiling({
          ...profiling,
          [profilingId]: {
            ...profiling[profilingId],
            latestResponse: response,
          },
        });
        if (response.status === "pending") {
          setTimeout(checkProfilingStatusLoop, 1000);
        }
      };
      await checkProfilingStatusLoop();
    }
  };

  const killActor = () => {
    if (actor.state === ActorState.Alive) {
      launchKillActor(actor.actorId, actor.ipAddress, actor.port);
    }
  };

  // Construct the custom message from the actor.
  let actorCustomDisplay: JSX.Element[] = [];
  if (isFullActorInfo(actor) && actor.webuiDisplay) {
    actorCustomDisplay = Object.keys(actor.webuiDisplay)
      .sort()
      .map((key, _, __) => {
        // Construct the value from actor.
        // Please refer to worker.py::show_in_webui for schema.
        const valueEncoded = actor.webuiDisplay![key];
        const valueParsed = JSON.parse(valueEncoded);
        let valueRendered = valueParsed["message"];
        if (valueParsed["dtype"] === "html") {
          valueRendered = (
            <div
              className={classes.inlineHTML}
              dangerouslySetInnerHTML={{ __html: valueRendered }}
            ></div>
          );
        }

        if (key === "") {
          return (
            <Typography className={classes.webuiDisplay}>
              &nbsp; &nbsp; {valueRendered}
            </Typography>
          );
        } else {
          return (
            <Typography className={classes.webuiDisplay}>
              &nbsp; &nbsp; {key}: {valueRendered}
            </Typography>
          );
        }
      });
  }

  return (
    <div className={classes.root}>
      <Typography className={classes.title}>
        {isFullActorInfo(actor) ? (
          <React.Fragment>
            Actor {actor.actorId} (Profile for
            {[10, 30, 60].map((duration) => (
              <React.Fragment key={duration}>
                {" "}
                <span
                  className={classes.action}
                  onClick={handleProfilingClick(duration)}
                >
                  {duration}s
                </span>
              </React.Fragment>
            ))}
            ){" "}
            {actor.state === ActorState.Alive && (
              <span className={classes.action} onClick={killActor}>
                Kill Actor
              </span>
            )}
            {Object.entries(profiling).map(
              ([profilingId, { startTime, latestResponse }]) =>
                latestResponse !== null && (
                  <React.Fragment key={profilingId}>
                    (
                    {latestResponse.status === "pending" ? (
                      `Profiling for ${Math.round(
                        (Date.now() - startTime) / 1000,
                      )}s...`
                    ) : latestResponse.status === "finished" ? (
                      <a
                        className={classes.action}
                        href={getProfilingResultURL(profilingId)}
                        rel="noopener noreferrer"
                        target="_blank"
                      >
                        Profiling result
                      </a>
                    ) : latestResponse.status === "error" ? (
                      `Profiling error: ${latestResponse.error.trim()}`
                    ) : undefined}
                    ){" "}
                  </React.Fragment>
                ),
            )}
          </React.Fragment>
        ) : actor.state === ActorState.Infeasible ? (
          <span className={classes.infeasible}>
            {actor.actorClass} cannot be created because the Ray cluster cannot
            satisfy its resource requirements.
          </span>
        ) : (
          <span className={classes.pendingResources}>
            {actor.actorClass} is pending until resources are available.
          </span>
        )}
      </Typography>
      <ActorDetailsPane actor={actor} />
      {isFullActorInfo(actor) && (
        <React.Fragment>
          {actorCustomDisplay.length > 0 && (
            <React.Fragment>{actorCustomDisplay}</React.Fragment>
          )}
        </React.Fragment>
      )}
    </div>
  );
};

export default Actor;
