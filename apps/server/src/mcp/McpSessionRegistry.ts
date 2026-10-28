import {
  OrchestratorMcpFailure,
  type RuntimeMode,
  type ProviderInteractionMode,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { HttpServer } from "effect/http";
import * as NetAddress from "effect/net/NetAddress";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import * as McpProviderSession from "@t3tools/provider-core/server/mcpSession";

export interface McpCredentialRequest {
  readonly threadId: ThreadId;
  readonly providerInstanceId: ProviderInstanceId;
  /** False omits the preview capability wherever the token is honored. Defaults to full access. */
  readonly browserToolsAvailable?: boolean;
  readonly capabilities?: ReadonlySet<McpInvocationContext.McpCapability>;
  readonly subagent?: boolean;
}

export interface McpIssuedCredential {
  readonly config: McpProviderSession.McpProviderSessionConfig;
}

export interface FleetLaunchGrant {
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly requestDigest: string;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

export interface McpSessionRegistryShape {
  readonly authorizeFleetLaunch: (
    grant: FleetLaunchGrant,
  ) => Effect.Effect<string, OrchestratorMcpFailure>;
  readonly consumeFleetLaunch: (
    authorization: string,
    requestDigest: string,
  ) => Effect.Effect<FleetLaunchGrant, OrchestratorMcpFailure>;
  readonly issue: (request: McpCredentialRequest) => Effect.Effect<McpIssuedCredential>;
  readonly resolve: (
    rawToken: string,
  ) => Effect.Effect<McpInvocationContext.McpThreadInvocationScope | undefined>;
  readonly touch: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeProviderSession: (providerSessionId: string) => Effect.Effect<void>;
  readonly revokeThread: (threadId: ThreadId) => Effect.Effect<void>;
  readonly revokeAll: Effect.Effect<void>;
}

export class McpSessionRegistry extends Context.Service<
  McpSessionRegistry,
  McpSessionRegistryShape
>()("t3/mcp/McpSessionRegistry") {}

interface CredentialRecord {
  readonly tokenHash: string;
  readonly scope: McpInvocationContext.McpThreadInvocationScope;
  readonly lastAliveAt: number;
}

interface RegistryState {
  readonly records: ReadonlyMap<string, CredentialRecord>;
  readonly grants: ReadonlyMap<string, FleetLaunchGrant & { readonly expiresAt: number }>;
}

export interface McpSessionRegistryOptions {
  readonly livenessWindowMs?: number;
  readonly now?: () => number;
}

/**
 * `/mcp` is outside environment auth, so idle tokens stop authenticating.
 * Keep expired records until explicit revocation: a trusted provider turn can
 * refresh the token its subprocess still holds; MCP requests cannot.
 */
const DEFAULT_LIVENESS_WINDOW_MS = 24 * 60 * 60 * 1_000;

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const tokenFromBytes = (bytes: Uint8Array): string => Buffer.from(bytes).toString("base64url");

const getHttpMcpEndpointHost = (address: NetAddress.IpAddress): string =>
  NetAddress.isUnspecified(address)
    ? "127.0.0.1"
    : NetAddress.formatUrlHostString(NetAddress.formatIp(address));

const makeWithOptions = Effect.fn("McpSessionRegistry.make")(function* (
  options: McpSessionRegistryOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const environmentId = yield* environment.getEnvironmentId;
  const httpServer = yield* HttpServer.HttpServer;
  const state = yield* SynchronizedRef.make<RegistryState>({
    records: new Map(),
    grants: new Map(),
  });
  const currentTimeMillis = options.now ? Effect.sync(options.now) : Clock.currentTimeMillis;
  const livenessWindowMs = options.livenessWindowMs ?? DEFAULT_LIVENESS_WINDOW_MS;
  const endpoint = NetAddress.isInetAddress(httpServer.address)
    ? `http://${getHttpMcpEndpointHost(httpServer.address.address)}:${httpServer.address.port}/mcp`
    : "http://127.0.0.1/mcp";

  const hashToken = (token: string) =>
    crypto
      .digest("SHA-256", new TextEncoder().encode(token))
      .pipe(Effect.map(bytesToHex), Effect.orDie);

  const issue: McpSessionRegistryShape["issue"] = Effect.fn("McpSessionRegistry.issue")(
    function* (request) {
      const issuedAt = yield* currentTimeMillis;
      const providerSessionId = yield* crypto.randomUUIDv4().pipe(Effect.orDie);
      const rawToken = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
      const tokenHash = yield* hashToken(rawToken);
      const browserToolsAvailable = request.browserToolsAvailable ?? true;
      const scope: McpInvocationContext.McpThreadInvocationScope = {
        environmentId,
        requestNamespace: providerSessionId,
        thread: {
          threadId: ThreadId.make(request.threadId),
          providerSessionId,
          providerInstanceId: ProviderInstanceId.make(request.providerInstanceId),
          subagent: request.subagent === true,
        },
        client: undefined,
        capabilities: new Set<McpInvocationContext.McpCapability>([
          "orchestration",
          "worktree",
          "pull-requests",
          ...(request.capabilities ?? (browserToolsAvailable ? (["preview"] as const) : [])),
        ]),
        issuedAt,
      };
      yield* SynchronizedRef.update(state, ({ records, grants }) => {
        const next = new Map(records);
        next.set(tokenHash, { tokenHash, scope, lastAliveAt: issuedAt });
        return { records: next, grants };
      });
      return {
        config: {
          environmentId,
          threadId: scope.thread.threadId,
          providerSessionId,
          providerInstanceId: scope.thread.providerInstanceId,
          endpoint,
          authorizationHeader: `Bearer ${rawToken}`,
          browserToolsAvailable: scope.capabilities.has("preview"),
          capabilities: scope.capabilities,
        },
      };
    },
  );

  const resolve: McpSessionRegistryShape["resolve"] = Effect.fn("McpSessionRegistry.resolve")(
    function* (rawToken) {
      if (rawToken.length === 0) return undefined;
      const tokenHash = yield* hashToken(rawToken);
      const timestamp = yield* currentTimeMillis;
      return yield* SynchronizedRef.modify(state, ({ records, grants }) => {
        const record = records.get(tokenHash);
        if (!record || timestamp - record.lastAliveAt > livenessWindowMs) {
          return [undefined, { records, grants }] as const;
        }
        const next = new Map(records);
        next.set(tokenHash, { ...record, lastAliveAt: timestamp });
        return [record.scope, { records: next, grants }] as const;
      });
    },
  );

  const touch: McpSessionRegistryShape["touch"] = Effect.fn("McpSessionRegistry.touch")(
    function* (threadId) {
      const timestamp = yield* currentTimeMillis;
      yield* SynchronizedRef.update(state, ({ records, grants }) => {
        const next = new Map(records);
        for (const [tokenHash, record] of records) {
          if (record.scope.thread.threadId === threadId) {
            next.set(tokenHash, { ...record, lastAliveAt: timestamp });
          }
        }
        return { records: next, grants };
      });
    },
  );

  const revokeWhere = (predicate: (record: CredentialRecord) => boolean) =>
    SynchronizedRef.update(state, ({ records, grants }) => ({
      records: new Map(Array.from(records).filter(([, record]) => !predicate(record))),
      grants,
    }));

  const grantRefused = () =>
    new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Fleet launch authorization is invalid, expired, consumed, or revoked.",
    });
  const authorizeFleetLaunch: McpSessionRegistryShape["authorizeFleetLaunch"] = Effect.fn(
    "McpSessionRegistry.authorizeFleetLaunch",
  )(function* (grant) {
    const now = yield* currentTimeMillis;
    const token = yield* crypto.randomBytes(32).pipe(Effect.map(tokenFromBytes), Effect.orDie);
    const hash = yield* hashToken(token);
    const accepted = yield* SynchronizedRef.modify(state, ({ records, grants }) => {
      const next = new Map(Array.from(grants).filter(([, value]) => value.expiresAt > now));
      const live = Array.from(records.values()).some(
        (record) =>
          record.scope.thread.providerSessionId === grant.scope.thread.providerSessionId &&
          now - record.lastAliveAt <= livenessWindowMs,
      );
      if (!live || next.size >= 256) return [false, { records, grants: next }] as const;
      next.set(hash, { ...grant, expiresAt: now + 30_000 });
      return [true, { records, grants: next }] as const;
    });
    if (!accepted) return yield* grantRefused();
    return token;
  });
  const consumeFleetLaunch: McpSessionRegistryShape["consumeFleetLaunch"] = Effect.fn(
    "McpSessionRegistry.consumeFleetLaunch",
  )(function* (authorization, requestDigest) {
    const hash = yield* hashToken(authorization);
    const now = yield* currentTimeMillis;
    const grant = yield* SynchronizedRef.modify(state, ({ records, grants }) => {
      const value = grants.get(hash);
      const next = new Map(
        Array.from(grants).filter(([key, item]) => key !== hash && item.expiresAt > now),
      );
      const live =
        value !== undefined &&
        Array.from(records.values()).some(
          (record) =>
            record.scope.thread.providerSessionId === value.scope.thread.providerSessionId &&
            now - record.lastAliveAt <= livenessWindowMs,
        );
      return [
        live && value.expiresAt > now && value.requestDigest === requestDigest ? value : undefined,
        { records, grants: next },
      ] as const;
    });
    if (grant === undefined) return yield* grantRefused();
    return grant;
  });

  return McpSessionRegistry.of({
    authorizeFleetLaunch,
    consumeFleetLaunch,
    issue,
    resolve,
    touch,
    revokeProviderSession: Effect.fn("McpSessionRegistry.revokeProviderSession")(
      function* (providerSessionId) {
        yield* revokeWhere((record) => record.scope.thread.providerSessionId === providerSessionId);
      },
    ),
    revokeThread: Effect.fn("McpSessionRegistry.revokeThread")(function* (threadId) {
      yield* revokeWhere((record) => record.scope.thread.threadId === threadId);
    }),
    revokeAll: SynchronizedRef.set(state, { records: new Map(), grants: new Map() }),
  });
});

let activeMcpSessionRegistry: McpSessionRegistryShape | undefined;

const make = Effect.acquireRelease(
  makeWithOptions().pipe(
    Effect.tap((registry) =>
      Effect.sync(() => {
        activeMcpSessionRegistry = registry;
      }),
    ),
  ),
  (registry) =>
    Effect.sync(() => {
      if (activeMcpSessionRegistry === registry) {
        activeMcpSessionRegistry = undefined;
      }
    }),
);

export const layer = Layer.effect(McpSessionRegistry, make);

export const touchActiveMcpThread = (threadId: ThreadId): Effect.Effect<void> =>
  activeMcpSessionRegistry ? activeMcpSessionRegistry.touch(threadId) : Effect.void;

export const __testing = {
  make: makeWithOptions,
};
