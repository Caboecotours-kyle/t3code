import { OrchestratorMcpFailure, RuntimeMode, ProviderInteractionMode } from "@t3tools/contracts";
import * as FleetLaunch from "../../FleetLaunch.ts";
import * as Schema from "effect/Schema";
import { Tool, Toolkit } from "effect/ai";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpSessionRegistry from "../../McpSessionRegistry.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";

const digest = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const authorization = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/));
const modes = { runtimeMode: RuntimeMode, interactionMode: ProviderInteractionMode };
const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    McpSessionRegistry.McpSessionRegistry,
    ThreadManagement.ThreadManagementService,
  ],
};
export const FleetToolkit = Toolkit.make(
  Tool.make("t3_fleet_launch", {
    ...shared,
    description:
      "Launch one fleet builder as this calling thread. Pass requestJson, requestDigest and explicit modes from the installed fleet bridge's prepare-launch command. Native T3 holds the caller grant internally and dispatches the fixed bridge once. Modes cannot exceed yours. Retain threadId; on a lost reply reconcile fleet_threads by the exact unique title before another launch.",
    parameters: Schema.Struct({
      requestDigest: digest,
      requestJson: Schema.String.check(Schema.isMaxLength(120000)),
      ...modes,
    }),
    success: FleetLaunch.LaunchResult,
    dependencies: [...shared.dependencies, FleetLaunch.FleetLaunch],
  }).annotate(Tool.Destructive, true),
  Tool.make("t3_fleet_launch_consume", {
    ...shared,
    description:
      "Fleet bridge only: consume an exact-request launch authorization. Requires an owner client; rechecks the original thread's live session and modes. Returns no provider credential.",
    parameters: Schema.Struct({ callerAuthorization: authorization, requestDigest: digest }),
    success: Schema.Struct(modes),
  }).annotate(Tool.Destructive, true),
);
