import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as FleetLaunch from "../../FleetLaunch.ts";
import * as Effect from "effect/Effect";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpSessionRegistry from "../../McpSessionRegistry.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import {
  assertLiveCaller,
  assertTargetWithinLimits,
  readCaller,
  type Caller,
} from "../../threadAccess.ts";
import { callerLinkOrigin } from "../../linkOrigin.ts";
import { FleetToolkit } from "./tools.ts";

const requireLocalCaller = (caller: Caller) =>
  callerLinkOrigin(caller) === undefined
    ? Effect.void
    : Effect.fail(
        new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Linked callers cannot authorize another fleet launch.",
        }),
      );

export const layer = McpToolAccess.toLayer(FleetToolkit, {
  t3_fleet_launch: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const caller = yield* readCaller();
      yield* requireLocalCaller(caller);
      const scope = yield* McpInvocationContext.requireThreadScope(
        caller.scope,
        "Fleet launch authorization",
      );
      yield* assertTargetWithinLimits(caller.limits, input);
      const fleet = yield* FleetLaunch.FleetLaunch;
      return yield* fleet.launch({
        scope,
        requestDigest: input.requestDigest,
        requestJson: input.requestJson,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
      });
    }),
  ),
  t3_fleet_launch_consume: McpToolAccess.writesEnvironment((input) =>
    Effect.gen(function* () {
      const consumer = yield* readCaller();
      yield* requireLocalCaller(consumer);
      if (consumer.scope.client === undefined)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Only a local owner client can redeem fleet launch authorization.",
        });
      const registry = yield* McpSessionRegistry.McpSessionRegistry;
      const grant = yield* registry.consumeFleetLaunch(
        input.callerAuthorization,
        input.requestDigest,
      );
      const caller = yield* readCaller().pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, grant.scope),
      );
      yield* assertLiveCaller(caller);
      yield* requireLocalCaller(caller);
      yield* assertTargetWithinLimits(caller.limits, grant);
      return { runtimeMode: grant.runtimeMode, interactionMode: grant.interactionMode };
    }),
  ),
});
