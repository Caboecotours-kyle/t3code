import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  type RuntimeMode,
  type ProviderInteractionMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpSessionRegistry from "../../McpSessionRegistry.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as Handlers from "./handlers.ts";
import { FleetToolkit } from "./tools.ts";

const threadId = ThreadId.make("fleet-source");
const scope: McpInvocationContext.McpThreadInvocationScope = {
  environmentId: EnvironmentId.make("source"),
  requestNamespace: "source-session",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
  client: undefined,
  thread: {
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "source-session",
  },
};
const owner: McpInvocationContext.McpInvocationScope = {
  ...scope,
  thread: undefined,
  client: { sessionId: "owner", label: "fleet", access: "full-access" },
};

it.effect(
  "fleet authorization rejects owner minting, live mode escalation, ended callers and linked deputies",
  () =>
    Effect.gen(function* () {
      const modes: {
        runtimeMode: RuntimeMode;
        interactionMode: ProviderInteractionMode;
        active: boolean;
      } = {
        runtimeMode: "auto",
        interactionMode: "plan",
        active: true,
      };
      let authorizations = 0;
      const layers = Layer.mergeAll(
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: (id) =>
            Effect.succeed(
              liveThreadShell(id, { ...modes, activeRunId: modes.active ? undefined : null }),
            ),
        }),
        Layer.mock(PeerForwarding.PeerForwarding)({}),
        Layer.mock(McpSessionRegistry.McpSessionRegistry)({
          authorizeFleetLaunch: () => {
            authorizations++;
            return Effect.succeed("a".repeat(43));
          },
          consumeFleetLaunch: () =>
            Effect.succeed({
              scope,
              requestDigest: "b".repeat(64),
              runtimeMode: "auto",
              interactionMode: "plan",
            }),
        }),
      );
      const withScope = (caller: McpInvocationContext.McpInvocationScope) =>
        Layer.merge(layers, Layer.succeed(McpInvocationContext.McpInvocationContext, caller));
      const call = (
        caller: McpInvocationContext.McpInvocationScope,
        name: "t3_fleet_launch_authorize" | "t3_fleet_launch_consume",
        params: unknown,
      ) =>
        Effect.gen(function* () {
          const dependencies = withScope(caller);
          const toolkit = yield* FleetToolkit.pipe(
            Effect.provide(
              McpToolAccess.HandlersLayer.layer(Handlers.layer).pipe(Layer.provide(dependencies)),
            ),
          );
          const results = yield* toolkit
            .handle(name, params)
            .pipe(Stream.unwrap, Stream.runCollect, Effect.provide(dependencies));
          return results.at(-1);
        });
      const input = {
        requestDigest: "b".repeat(64),
        runtimeMode: "auto" as const,
        interactionMode: "plan" as const,
      };
      expect((yield* call(owner, "t3_fleet_launch_authorize", input))?.isFailure).toBe(true);
      expect(authorizations).toBe(0);
      expect(
        (yield* call(scope, "t3_fleet_launch_authorize", { ...input, runtimeMode: "full-access" }))
          ?.isFailure,
      ).toBe(true);
      expect(
        (yield* call(scope, "t3_fleet_launch_authorize", { ...input, interactionMode: "default" }))
          ?.isFailure,
      ).toBe(true);
      expect((yield* call(scope, "t3_fleet_launch_authorize", input))?.isFailure).toBe(false);
      const redeem = { callerAuthorization: "a".repeat(43), requestDigest: input.requestDigest };
      expect((yield* call(owner, "t3_fleet_launch_consume", redeem))?.isFailure).toBe(false);
      modes.runtimeMode = "approval-required";
      expect((yield* call(owner, "t3_fleet_launch_consume", redeem))?.isFailure).toBe(true);
      modes.runtimeMode = "auto";
      modes.active = false;
      expect((yield* call(owner, "t3_fleet_launch_consume", redeem))?.isFailure).toBe(true);
      expect((yield* call(scope, "t3_fleet_launch_authorize", input))?.isFailure).toBe(true);
      modes.active = true;
      const linked = {
        ...owner,
        client: {
          sessionId: "linked",
          label: "peer",
          access: "full-access" as const,
          linked: true,
        },
      };
      expect((yield* call(linked, "t3_fleet_launch_consume", redeem))?.isFailure).toBe(true);
    }),
);
