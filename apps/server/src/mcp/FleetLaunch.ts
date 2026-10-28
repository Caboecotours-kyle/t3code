import {
  OrchestratorMcpFailure,
  OrchestrationV2RunStatus,
  ProjectId,
  ThreadId,
  RunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as ProcessRunner from "../processRunner.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

export const LaunchResult = Schema.Struct({
  threadId: ThreadId,
  projectId: ProjectId,
  runId: Schema.NullOr(RunId),
  status: Schema.NullOr(OrchestrationV2RunStatus),
});

export class FleetLaunch extends Context.Service<
  FleetLaunch,
  {
    readonly launch: (
      input: McpSessionRegistry.FleetLaunchGrant & { readonly requestJson: string },
    ) => Effect.Effect<typeof LaunchResult.Type, OrchestratorMcpFailure>;
  }
>()("t3/mcp/FleetLaunch") {}

const invalidRequest = () =>
  new OrchestratorMcpFailure({
    code: "invalid_request",
    message: "Invalid fleet launch request. Prepare it with the installed fleet bridge.",
  });
const uncertain = () =>
  new OrchestratorMcpFailure({
    code: "orchestration_error",
    message:
      "Fleet launch did not return a confirmed result. Reconcile fleet_threads by exact title before another launch.",
  });
const decodeRequest = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeReply = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      isError: Schema.Boolean,
      structuredContent: Schema.Unknown,
    }),
  ),
);
const decodeResult = Schema.decodeUnknownEffect(LaunchResult);
const encodeRequest = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const make = Effect.gen(function* () {
  const registry = yield* McpSessionRegistry.McpSessionRegistry;
  const runner = yield* ProcessRunner.ProcessRunner;
  const bridge = NodePath.join(NodeOS.homedir(), ".local/share/t3-fleet-mcp/t3-fleet-mcp.py");
  const launch: FleetLaunch["Service"]["launch"] = Effect.fn("FleetLaunch.launch")(
    function* (input) {
      const request = yield* decodeRequest(input.requestJson).pipe(Effect.mapError(invalidRequest));
      if (
        "callerAuthorization" in request ||
        request.runtimeMode !== input.runtimeMode ||
        request.interactionMode !== input.interactionMode
      )
        return yield* invalidRequest();
      if (Buffer.byteLength(input.requestJson) > 120_000) return yield* invalidRequest();
      // The model never receives this grant: provider transcripts and turn-item persistence cannot redact it reliably.
      const callerAuthorization = yield* registry.authorizeFleetLaunch({
        scope: input.scope,
        requestDigest: input.requestDigest,
        runtimeMode: input.runtimeMode,
        interactionMode: input.interactionMode,
      });
      const stdin = encodeRequest({ ...request, callerAuthorization });
      if (Buffer.byteLength(stdin) > 128 * 1024) return yield* invalidRequest();
      const response = yield* runner
        .run({
          command: "/usr/bin/python3",
          args: [bridge, "call", "fleet_launch", "-"],
          stdin,
          timeout: "48 seconds",
          maxOutputBytes: 256 * 1024,
          outputMode: "error",
        })
        .pipe(Effect.mapError(uncertain));
      if (
        response.code !== 0 ||
        response.timedOut ||
        response.stdoutInvalidUtf8 ||
        response.stdoutTruncated
      )
        return yield* uncertain();
      const reply = yield* decodeReply(response.stdout).pipe(Effect.mapError(uncertain));
      if (reply.isError) return yield* uncertain();
      return yield* decodeResult(reply.structuredContent).pipe(Effect.mapError(uncertain));
    },
  );
  return FleetLaunch.of({ launch });
});

export const layer = Layer.effect(FleetLaunch, make);
