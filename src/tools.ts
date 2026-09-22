import type { EndpointRow, Role } from "./db.js";
import { writeErrorRecord } from "./one-line.js";
import {
  BridgeBus,
  BridgeTransitionError,
  DEFAULT_FETCH_LIMIT,
  MAX_FETCH_LIMIT,
  oppositeRole,
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
const REMOVED_SEND_ARGUMENTS = [
  "to_tag",
  "broadcast",
  "on_timeout",
  "to_endpoint",
] as const;

export const TOOL_DEFINITIONS = [
  {
    name: "bridge_send",
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: false,
      openWorldHint: false,
    },
    description:
      "Store one message for one or more registered endpoints of the opposite role. One call writes one message and one pending delivery per name. Resending the same message_id adds only missing destinations.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        subject: {
          type: "string",
          description: "Subject normalized to 1-500 UTF-8 bytes.",
        },
        body: {
          type: "string",
          minLength: 1,
          description: "Message body, limited to 262144 UTF-8 bytes.",
        },
        message_id: {
          type: "string",
          pattern: MESSAGE_ID_PATTERN,
          description: "Optional caller-supplied RFC 4122 UUID idempotency key.",
        },
        thread_id: {
          type: "string",
          description: "Authoritative sender thread identifier supplied by the caller.",
        },
        to_endpoints: {
          type: "array",
          minItems: 1,
          items: { type: "string" },
          description:
            "Registered destination endpoint names. Role must be the opposite of this server, names must be unique, and none may be retired.",
        },
      },
      required: ["subject", "body", "to_endpoints"],
    },
  },
  {
    name: "bridge_fetch",
    annotations: {
      readOnlyHint: false,
      destructiveHint: true,
      idempotentHint: false,
      openWorldHint: false,
    },
    description:
      "Fetch pending deliveries for this server's endpoint. peek=true is read-only and pages by delivery_id.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        peek: { type: "boolean", default: false },
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
            "Fetch this one message instead of the oldest visible deliveries.",
        },
        cursor: {
          type: "integer",
          minimum: 1,
          description:
            "Continue a peek after next_cursor. Only valid with peek.",
        },
      },
    },
  },
  {
    name: "bridge_ack",
    annotations: {
      readOnlyHint: false,
      destructiveHint: true,
      idempotentHint: true,
      openWorldHint: false,
    },
    description:
      "Acknowledge a delivery only when message_id, attempt_id, and this process's holder all match.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        message_id: { type: "string", pattern: MESSAGE_ID_PATTERN },
        attempt_id: { type: "string", pattern: ATTEMPT_ID_PATTERN },
      },
      required: ["message_id", "attempt_id"],
    },
  },
  {
    name: "bridge_status",
    annotations: {
      readOnlyHint: true,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: false,
    },
    description:
      "Read one message's deliveries and its event history. There is no single top-level status.",
    inputSchema: {
      type: "object",
      additionalProperties: false,
      properties: {
        message_id: { type: "string", pattern: MESSAGE_ID_PATTERN },
      },
      required: ["message_id"],
    },
  },
] as const;

type JsonObject = Record<string, unknown>;

function textResult(text: string, isError = false): ToolCallResult {
  return {
    content: [{ type: "text", text }],
    ...(isError ? { isError: true } : {}),
  };
}

function requireObject(value: unknown): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("tool arguments must be an object");
  }
  return value as JsonObject;
}

function assertOnlyKeys(value: JsonObject, allowed: readonly string[]): void {
  const unexpected = Object.keys(value).filter((key) => !allowed.includes(key));
  if (unexpected.length > 0) {
    throw new Error(`unexpected tool argument(s): ${unexpected.join(", ")}`);
  }
}

function requiredString(value: JsonObject, key: string): string {
  const field = value[key];
  if (typeof field !== "string") {
    throw new Error(`${key} must be a string`);
  }
  return field;
}

function optionalString(value: JsonObject, key: string): string | undefined {
  const field = value[key];
  if (field === undefined) return undefined;
  if (typeof field !== "string") {
    throw new Error(`${key} must be a string when provided`);
  }
  return field;
}

function optionalBoolean(value: JsonObject, key: string): boolean | undefined {
  const field = value[key];
  if (field === undefined) return undefined;
  if (typeof field !== "boolean") {
    throw new Error(`${key} must be a boolean when provided`);
  }
  return field;
}

function optionalInteger(value: JsonObject, key: string): number | undefined {
  const field = value[key];
  if (field === undefined) return undefined;
  if (typeof field !== "number" || !Number.isInteger(field)) {
    throw new Error(`${key} must be an integer when provided`);
  }
  return field;
}

function stringArray(value: JsonObject, key: string): string[] {
  const field = value[key];
  if (!Array.isArray(field) || field.some((item) => typeof item !== "string")) {
    throw new Error(`${key} must be an array of strings`);
  }
  return field as string[];
}

function errorText(error: unknown): string {
  if (error instanceof BridgeTransitionError) {
    return `${error.message}; latest=${JSON.stringify(error.latest)}`;
  }
  return error instanceof Error ? error.message : String(error);
}

export class BridgeTools {
  constructor(
    private readonly bus: BridgeBus,
    private readonly role: Role,
    private readonly consumer: string,
    private readonly session: SessionTagState = { tag: null },
    private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly endpoint: EndpointRow | null = null,
  ) {
    void this.session;
    void this.env;
  }

  async call(name: string, rawArguments: unknown): Promise<ToolCallResult> {
    try {
      const args = requireObject(rawArguments ?? {});
      switch (name) {
        case "bridge_send":
          return this.bridgeSend(args);
        case "bridge_fetch":
          return this.bridgeFetch(args);
        case "bridge_ack":
          return this.bridgeAck(args);
        case "bridge_status":
          return this.bridgeStatus(args);
        default:
          throw new Error(`unknown tool: ${name}`);
      }
    } catch (error) {
      return textResult(`bridge tool error: ${errorText(error)}`, true);
    }
  }

  private bridgeSend(args: JsonObject): ToolCallResult {
    const removed = REMOVED_SEND_ARGUMENTS.filter((key) => key in args);
    if (removed.length > 0) {
      throw new Error(`refusing removed argument: ${removed.join(", ")}`);
    }
    assertOnlyKeys(args, ["subject", "body", "message_id", "thread_id", "to_endpoints"]);
    const subject = requiredString(args, "subject");
    const body = requiredString(args, "body");
    const messageId = optionalString(args, "message_id");
    const argumentThreadId = optionalString(args, "thread_id");
    const toEndpoints = stringArray(args, "to_endpoints");
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
      toEndpoints,
      sourceEndpoint: this.endpoint,
    });
    const added = result.added ?? [];
    return textResult(
      `bridge 送信: ${result.messageId} ${result.subject}${result.idempotent ? " (idempotent)" : ""}
宛先 endpoint: ${oppositeRole(this.role)}/${JSON.stringify(toEndpoints)}
added: ${JSON.stringify(added)}`,
    );
  }

  private bridgeFetch(args: JsonObject): ToolCallResult {
    assertOnlyKeys(args, ["peek", "limit", "message_id", "cursor"]);
    const peek = optionalBoolean(args, "peek") ?? false;
    const limit = optionalInteger(args, "limit") ?? DEFAULT_FETCH_LIMIT;
    const messageId = optionalString(args, "message_id");
    const cursor = optionalInteger(args, "cursor");
    const result = this.bus.fetch(this.role, this.consumer, {
      peek,
      limit,
      messageId,
      cursor,
      endpoint: this.endpoint,
    });
    const notices: string[] = [];
    if (peek) {
      notices.push(
        "PEEK（状態不変・ack されるまで再表示されます）",
        "本文は返していません。返るのは subject・from_endpoint・body_bytes です。",
      );
    }
    const prefix = notices.length > 0 ? `${notices.join("\n")}\n` : "";
    return textResult(`${prefix}${JSON.stringify(result, null, 2)}`);
  }

  private bridgeAck(args: JsonObject): ToolCallResult {
    assertOnlyKeys(args, ["message_id", "attempt_id"]);
    const messageId = requiredString(args, "message_id");
    const attemptId = requiredString(args, "attempt_id");
    const result = this.bus.ack(
      this.role,
      messageId,
      attemptId,
      undefined,
      this.consumer,
      this.endpoint,
    );
    return textResult(`bridge ack: ${result.message_id} ${result.attempt_id}`);
  }

  private bridgeStatus(args: JsonObject): ToolCallResult {
    assertOnlyKeys(args, ["message_id"]);
    return textResult(
      JSON.stringify(this.bus.status(requiredString(args, "message_id")), null, 2),
    );
  }
}