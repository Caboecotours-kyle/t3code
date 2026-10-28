import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ProcessRunner from "../processRunner.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as FleetLaunch from "./FleetLaunch.ts";

const scope: McpInvocationContext.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("source"),
  requestNamespace: "provider-session",
  issuedAt: 0,
  thread: {
    threadId: ThreadId.make("source-thread"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "provider-session",
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
};
const modes = { runtimeMode: "auto" as const, interactionMode: "plan" as const };
const input = {
  scope,
  requestDigest: "b".repeat(64),
  ...modes,
  requestJson: JSON.stringify({ host: "boat-proof", title: "unique proof", ...modes }),
};
const secret = "s".repeat(43);
const launched = {
  threadId: ThreadId.make("target-thread"),
  projectId: ProjectId.make("target-project"),
  runId: null,
  status: "preparing" as const,
};
const output = (stdout: string, code = 0) => ({
  stdout,
  stderr: "",
  code: code as ProcessRunner.ProcessRunOutput["code"],
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

it.effect(
  "native fleet launch keeps its grant inside fixed subprocess stdin and returns only launch fields",
  () =>
    Effect.gen(function* () {
      const calls: Array<ProcessRunner.ProcessRunInput> = [];
      const grants: Array<McpSessionRegistry.FleetLaunchGrant> = [];
      const layer = FleetLaunch.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(McpSessionRegistry.McpSessionRegistry)({
              authorizeFleetLaunch: (grant) => {
                grants.push(grant);
                return Effect.succeed(secret);
              },
            }),
            Layer.mock(ProcessRunner.ProcessRunner)({
              run: (call) => {
                calls.push(call);
                return Effect.succeed(
                  output(
                    JSON.stringify({
                      isError: false,
                      structuredContent: { ...launched, arbitrary: "discard" },
                    }),
                  ),
                );
              },
            }),
          ),
        ),
      );
      const service = yield* FleetLaunch.FleetLaunch.pipe(Effect.provide(layer));
      const result = yield* service.launch(input);
      expect(result).toEqual(launched);
      expect(JSON.stringify(result)).not.toContain(secret);
      expect(calls).toHaveLength(1);
      expect(calls[0]?.command).toBe("/usr/bin/python3");
      expect(calls[0]?.args.slice(-3)).toEqual(["call", "fleet_launch", "-"]);
      expect(JSON.stringify(calls[0]?.args)).not.toContain(secret);
      expect(JSON.parse(calls[0]?.stdin ?? "").callerAuthorization).toBe(secret);
      expect(grants[0]).not.toHaveProperty("requestJson");
      expect(grants[0]?.scope.thread.providerSessionId).toBe(scope.thread.providerSessionId);
    }),
);

it.effect(
  "native fleet launch rejects supplied grants and mode substitution; uncertain replies never leak child output or retry",
  () =>
    Effect.gen(function* () {
      let calls = 0;
      let grants = 0;
      const layer = FleetLaunch.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(McpSessionRegistry.McpSessionRegistry)({
              authorizeFleetLaunch: () => {
                grants++;
                return Effect.succeed(secret);
              },
            }),
            Layer.mock(ProcessRunner.ProcessRunner)({
              run: () => {
                calls++;
                return Effect.succeed(output(secret, 1));
              },
            }),
          ),
        ),
      );
      const service = yield* FleetLaunch.FleetLaunch.pipe(Effect.provide(layer));
      for (const request of [
        { ...modes, callerAuthorization: secret },
        { ...modes, runtimeMode: "full-access" },
      ]) {
        expect(
          (yield* service
            .launch({ ...input, requestJson: JSON.stringify(request) })
            .pipe(Effect.exit))._tag,
        ).toBe("Failure");
      }
      expect(grants).toBe(0);
      expect(calls).toBe(0);
      const uncertain = yield* service.launch(input).pipe(Effect.flip);
      expect(uncertain.message).toContain("Reconcile");
      expect(JSON.stringify(uncertain)).not.toContain(secret);
      expect(calls).toBe(1);
    }),
);
