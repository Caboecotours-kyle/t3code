import { OrchestratorMcpFailure, RuntimeMode, ProviderInteractionMode } from "@t3tools/contracts";
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
  Tool.make("t3_fleet_launch_authorize", {
    ...shared,
    description:
      "Authorize one fleet launch as this calling thread. Pass the SHA-256 digest from fleet launch preparation and explicit modes. The opaque authorization expires in 30 seconds and is consumed once; never log it. It cannot exceed your live modes. A lost launch reply needs reconciliation by title before another authorization.",
    parameters: Schema.Struct({ requestDigest: digest, ...modes }),
    success: Schema.Struct({ callerAuthorization: authorization }),
  }).annotate(Tool.Destructive, true),
  Tool.make("t3_fleet_launch_consume", {
    ...shared,
    description:
      "Fleet bridge only: consume an exact-request launch authorization. Requires an owner client; rechecks the original thread's live session and modes. Returns no provider credential.",
    parameters: Schema.Struct({ callerAuthorization: authorization, requestDigest: digest }),
    success: Schema.Struct(modes),
  }).annotate(Tool.Destructive, true),
);
