import type {
  EndpointRow,
  Role,
} from "./db.js";
import { writeErrorRecord } from "./one-line.js";
import {
  BridgeBus,
  BridgeTransitionError,
  DECLARED_TAG_ENV,
  DEFAULT_FETCH_LIMIT,
  MAX_FETCH_LIMIT,
  normalizeTag,
  oppositeRole,
  readDeclaredTag,
} from "./db.js";

export interface ToolCallResult {
  [key: string]: unknown;
  content: Array<{
    type: "text";
    text: string;
  }>;
  isError?: boolean;
}

export interface SessionTagState {
  tag: string | null;
}

const MESSAGE_ID_PATTERN =
  "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$";
const ATTEMPT_ID_PATTERN =
  "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$";

export const TOOL_DEFINITIONS = [
  {
    name: "bridge_hello",
    description:
      "Declare or replace this server process's session tag. The declaration is in memory and must be repeated after the MCP server restarts.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        tag: {
          type: "string",
          description:
            "Session tag normalized like subject and limited to 200 UTF-8 bytes.",
        },
      },
      required: ["tag"],
    },
  },
  {
    name: "bridge_send",
    description:
      "Store one message for the opposite bridge role. to_endpoint selects a registered destination endpoint. Without it, endpoint assignment is deferred. The response proves storage, not delivery.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        subject: {
          type: "string",
          description:
            "Subject normalized to 1-500 UTF-8 bytes.",
        },
        body: {
          type: "string",
          minLength: 1,
          description:
            "Message body, limited to 262144 UTF-8 bytes.",
        },
        message_id: {
          type: "string",
          pattern: MESSAGE_ID_PATTERN,
          description:
            "Optional caller-supplied RFC 4122 UUID idempotency key.",
        },
        thread_id: {
          type: "string",
          description:
            "Authoritative sender thread identifier supplied by the caller.",
        },
        to_tag: {
          type: "string",
          description:
            "Optional destination session tag, normalized to 1-200 UTF-8 bytes.",
        },
        to_endpoint: {
          type: "string",
          description:
            "Optional registered destination endpoint name. Its role must match the destination role and it must not be retired.",
        },
        broadcast: {
          type: "boolean",
          description:
            "State that the whole role is the intended destination. Required instead of to_tag when the destination role demands addressing. Cannot be combined with to_tag.",
        },
        on_timeout: {
          type: "string",
          enum: ["bounce", "fallback"],
          description:
            "Tagged-delivery timeout policy. Defaults to bounce and is invalid without to_tag.",
        },
      },
      required: ["subject", "body"],
    },
  },
  {
    name: "bridge_fetch",
    description:
      "Fetch messages visible to this process's declared tag. peek=true is read-only and uses the same visibility predicate.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        peek: {
          type: "boolean",
          default: false,
        },
        limit: {
          type: "integer",
          minimum: 1,
          maximum: MAX_FETCH_LIMIT,
          default: DEFAULT_FETCH_LIMIT,
        },
        message_id: {
          type: "string",
          pattern: MESSAGE_ID_PATTERN,
          description:
            "Fetch this one message instead of the oldest visible ones. A message that is not visible to this process is indistinguishable from one that does not exist.",
        },
        cursor: {
          type: "integer",
          minimum: 1,
          description:
            "Continue a peek after the value next_cursor returned. Peek changes nothing, so a repeated call without this returns the same page. Only valid with peek.",
        },
      },
    },
  },
  {
    name: "bridge_ack",
    description:
      "Acknowledge a message only when message_id and the current presented UUIDv4 attempt_id both match this role.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        message_id: {
          type: "string",
          pattern: MESSAGE_ID_PATTERN,
        },
        attempt_id: {
          type: "string",
          pattern: ATTEMPT_ID_PATTERN,
        },
      },
      required: ["message_id", "attempt_id"],
    },
  },
  {
    name: "bridge_status",
    description:
      "Read message status, delivery attempt count, timestamps, and event history.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        message_id: {
          type: "string",
          pattern: MESSAGE_ID_PATTERN,
        },
      },
      required: ["message_id"],
    },
  },
] as const;

type JsonObject = Record<string, unknown>;

function textResult(
  text: string,
  isError = false,
): ToolCallResult {
  return {
    content: [
      {
        type: "text",
        text,
      },
    ],
    ...(isError ? { isError: true } : {}),
  };
}

function requireObject(value: unknown): JsonObject {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    throw new Error(
      "tool arguments must be an object",
    );
  }

  return value as JsonObject;
}

function assertOnlyKeys(
  value: JsonObject,
  allowed: readonly string[],
): void {
  const unexpected = Object.keys(value).filter(
    (key) => !allowed.includes(key),
  );

  if (unexpected.length > 0) {
    throw new Error(
      `unexpected tool argument(s): ${unexpected.join(", ")}`,
    );
  }
}

function requiredString(
  value: JsonObject,
  key: string,
): string {
  const field = value[key];
  if (typeof field !== "string") {
    throw new Error(`${key} must be a string`);
  }

  return field;
}

function optionalString(
  value: JsonObject,
  key: string,
): string | undefined {
  const field = value[key];
  if (field === undefined) {
    return undefined;
  }

  if (typeof field !== "string") {
    throw new Error(
      `${key} must be a string when provided`,
    );
  }

  return field;
}

function optionalBoolean(
  value: JsonObject,
  key: string,
): boolean | undefined {
  const field = value[key];
  if (field === undefined) {
    return undefined;
  }

  if (typeof field !== "boolean") {
    throw new Error(
      `${key} must be a boolean when provided`,
    );
  }

  return field;
}

function optionalInteger(
  value: JsonObject,
  key: string,
): number | undefined {
  const field = value[key];
  if (field === undefined) {
    return undefined;
  }

  if (
    typeof field !== "number" ||
    !Number.isInteger(field)
  ) {
    throw new Error(
      `${key} must be an integer when provided`,
    );
  }

  return field;
}

function destinationNotice(
  toRole: Role,
  toEndpoint: string | null,
  toTag: string | null,
  destinationRequiresTag: boolean | null,
  broadcast: boolean | undefined,
  onTimeout: string | undefined,
): string {
  if (toEndpoint !== null) {
    return `宛先 endpoint: ${toRole}/${JSON.stringify(
      toEndpoint,
    )}`;
  }

  if (toTag !== null) {
    return onTimeout === "fallback"
      ? `宛先: ${toTag}。受領されないまま期限が過ぎると ${toRole} 役の全セッションへ降格する（on_timeout=fallback）。`
      : `宛先: ${toTag}`;
  }

  if (broadcast === true) {
    return `宛先: ${toRole} 役の全セッション（broadcast 指定）`;
  }

  if (destinationRequiresTag === null) {
    return `宛先: ${toRole} 役の全セッション`;
  }

  return destinationRequiresTag
    ? `宛先: ${toRole} 役の全セッション`
    : `宛先: ${toRole} 役の全セッション。require_tag はこの役に設定されていないので、宛先の指定は求められていません。`;
}

function errorText(error: unknown): string {
  if (
    error instanceof BridgeTransitionError
  ) {
    return `${error.message}; latest=${JSON.stringify(error.latest)}`;
  }

  return error instanceof Error
    ? error.message
    : String(error);
}

export class BridgeTools {
  constructor(
    private readonly bus: BridgeBus,
    private readonly role: Role,
    private readonly consumer: string,
    private readonly session: SessionTagState = {
      tag: null,
    },
    private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly endpoint: EndpointRow | null = null,
  ) {}

  async call(
    name: string,
    rawArguments: unknown,
  ): Promise<ToolCallResult> {
    try {
      const args = requireObject(
        rawArguments ?? {},
      );

      switch (name) {
        case "bridge_hello":
          return this.bridgeHello(args);
        case "bridge_send":
          return this.bridgeSend(args);
        case "bridge_fetch":
          return this.bridgeFetch(args);
        case "bridge_ack":
          return this.bridgeAck(args);
        case "bridge_status":
          return this.bridgeStatus(args);
        default:
          throw new Error(
            `unknown tool: ${name}`,
          );
      }
    } catch (error) {
      return textResult(
        `bridge tool error: ${errorText(error)}`,
        true,
      );
    }
  }

  private bridgeHello(
    args: JsonObject,
  ): ToolCallResult {
    assertOnlyKeys(args, ["tag"]);

    const tag = normalizeTag(
      requiredString(args, "tag"),
    );
    const previous = this.session.tag;
    this.session.tag = tag;

    return textResult(
      `bridge hello: ${tag}${
        previous === tag
          ? " (idempotent)"
          : previous === null
            ? ""
            : ` (renamed from ${previous})`
      }${this.describeEnvironmentTag(tag)}`,
    );
  }

  private describeEnvironmentTag(
    tag: string,
  ): string {
    const declared = readDeclaredTag(
      this.env,
    );

    if (declared.unusable !== null) {
      return `; ${declared.unusable} — hook はこのセッションを宛先なしとして数えるので、${tag} 宛の便は件数に出ない`;
    }

    if (declared.tag === null) {
      return `; ${DECLARED_TAG_ENV} はこのプロセスに渡っていない。hook 側の宣言と一致しているかはここからは分からない`;
    }

    return declared.tag === tag
      ? ""
      : `; ${DECLARED_TAG_ENV}=${JSON.stringify(
          declared.tag,
        )} と食い違っている。hook は env の値で数えるので、${tag} 宛の便は自分宛に数えられない`;
  }

  private bridgeSend(
    args: JsonObject,
  ): ToolCallResult {
    assertOnlyKeys(args, [
      "subject",
      "body",
      "message_id",
      "thread_id",
      "to_tag",
      "to_endpoint",
      "broadcast",
      "on_timeout",
    ]);

    const subject = requiredString(
      args,
      "subject",
    );
    const body = requiredString(args, "body");
    const messageId = optionalString(
      args,
      "message_id",
    );
    const argumentThreadId = optionalString(
      args,
      "thread_id",
    );
    const toTag = optionalString(
      args,
      "to_tag",
    );
    const toEndpoint =
      optionalString(
        args,
        "to_endpoint",
      ) ?? null;
    const onTimeout = optionalString(
      args,
      "on_timeout",
    );
    const broadcast = optionalBoolean(
      args,
      "broadcast",
    );

    if (
      argumentThreadId === undefined &&
      this.role === "codex" &&
      process.env.CODEX_THREAD_ID
    ) {
      writeErrorRecord(
        "agent-bridge: CODEX_THREAD_ID is set but thread_id was not passed; not recording it",
      );
    }

    const result = this.bus.send({
      fromRole: this.role,
      toRole: oppositeRole(this.role),
      subject,
      body,
      messageId,
      senderThreadId: argumentThreadId,
      toTag,
      toEndpoint,
      broadcast,
      onTimeout,
      fromTag: this.session.tag,
      sourceEndpoint: this.endpoint,
    });

    if ("kind" in result) {
      return textResult(
        `bridge tool error: ${result.reason}`,
        true,
      );
    }

    return textResult(
      `bridge 送信: ${result.messageId} ${result.subject}${
        result.idempotent
          ? " (idempotent)"
          : ""
      }
${destinationNotice(
        oppositeRole(this.role),
        toEndpoint,
        result.toTag,
        result.destinationRequiresTag,
        broadcast,
        onTimeout,
      )}`,
    );
  }

  private bridgeFetch(
    args: JsonObject,
  ): ToolCallResult {
    assertOnlyKeys(args, [
      "peek",
      "limit",
      "message_id",
      "cursor",
    ]);

    const peek =
      optionalBoolean(args, "peek") ?? false;
    const limit =
      optionalInteger(args, "limit") ??
      DEFAULT_FETCH_LIMIT;
    const messageId = optionalString(
      args,
      "message_id",
    );
    const cursor = optionalInteger(
      args,
      "cursor",
    );

    const result = this.bus.fetch(
      this.role,
      this.consumer,
      {
        peek,
        limit,
        tag: this.session.tag,
        messageId,
        cursor,
      },
    );

    const notices: string[] = [];

    if (peek) {
      notices.push(
        "PEEK（状態不変・ack されるまで再表示されます）",
        "本文は返していません。自分宛と判断できた便だけ、非 peek の bridge_fetch で本文を取ってください。",
      );
    }

    if (result.declared_tag === null) {
      notices.push(
        "このセッションはタグを宣言していません。名指しの便は見えません。bridge_hello を呼ぶと見えるようになります。",
      );
    }

    const prefix =
      notices.length > 0
        ? `${notices.join("\n")}\n`
        : "";

    return textResult(
      `${prefix}${JSON.stringify(result, null, 2)}`,
    );
  }

  private bridgeAck(
    args: JsonObject,
  ): ToolCallResult {
    assertOnlyKeys(args, [
      "message_id",
      "attempt_id",
    ]);

    const messageId = requiredString(
      args,
      "message_id",
    );
    const attemptId = requiredString(
      args,
      "attempt_id",
    );
    const result = this.bus.ack(
      this.role,
      messageId,
      attemptId,
      undefined,
      this.consumer,
    );

    return textResult(
      `bridge ack: ${result.message_id} ${result.attempt_id}`,
    );
  }

  private bridgeStatus(
    args: JsonObject,
  ): ToolCallResult {
    assertOnlyKeys(args, ["message_id"]);

    const messageId = requiredString(
      args,
      "message_id",
    );
    return textResult(
      JSON.stringify(
        this.bus.status(messageId),
        null,
        2,
      ),
    );
  }
}