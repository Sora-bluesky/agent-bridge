import {
  createHash,
  randomUUID,
} from "node:crypto";
import {
  existsSync,
  mkdirSync,
} from "node:fs";
import {
  dirname,
  join,
} from "node:path";
import Database from "better-sqlite3";

export const LEGACY_SCHEMA_VERSION = "3.2";
export const SCHEMA_VERSION = "4.0";
export const BUSY_TIMEOUT_MS = 5_000;
export const CLAIM_LEASE_MS = 120_000;
export const PRESENTED_TTL_MS = 15 * 60_000;
export const TAG_TTL_MS = 30 * 60_000;
export const DEFAULT_FETCH_LIMIT = 3;
export const MAX_FETCH_LIMIT = 10;

export const BOUNCE_NAMESPACE_UUID =
  "2fce6f02-4d78-4e23-9e04-a04e565f7c72";
export const BOUNCE_SUBJECT = "bridge: undelivered";
export const BOUNCE_REASON =
  "destination session tag expired before delivery";

export type Role = "claude" | "codex";
export type TimeoutPolicy = "bounce" | "fallback";
export type MessageStatus =
  | "stored"
  | "claimed"
  | "presented"
  | "acked"
  | "rejected"
  | "bounced";

export interface BridgeMetadata {
  dbPath: string;
  rootId: string;
  schemaVersion: string;
}

export interface MessageRow {
  id: number;
  message_id: string;
  root_id: string;
  from_role: Role;
  to_role: Role;
  to_tag: string | null;
  from_tag: string | null;
  on_timeout: TimeoutPolicy | null;
  tag_expires_at: number | null;
  subject: string;
  body: string;
  envelope_sha256: string;
  body_sha256: string;
  sender_thread_id: string | null;
  status: MessageStatus;
  attempt_id: string | null;
  consumer: string | null;
  lease_expires_at: number | null;
  attempt_count: number;
  sent_at: string;
  presented_at: string | null;
  acked_at: string | null;
}

export interface EventRow {
  seq: number;
  message_id: string | null;
  attempt_id: string | null;
  event: string;
  at: string;
  detail: string | null;
}

export interface ClaimedMessage extends MessageRow {
  status: "claimed";
  attempt_id: string;
  consumer: string;
  lease_expires_at: number;
  redelivery: boolean;
}

export interface FetchMessage {
  message_id: string;
  attempt_id: string | null;
  subject: string;
  to_tag: string | null;
  from_tag: string | null;
  body_bytes: number;
  body?: string;
  redelivery: boolean;
}

export interface FetchResult {
  declared_tag: string | null;
  messages: FetchMessage[];
  has_more: boolean;
  unacked_total: number;
  peek: boolean;
}

export interface SendResult {
  messageId: string;
  subject: string;
  idempotent: boolean;
  /*
   * Both are decided inside the send transaction and returned, so a
   * caller describing what it just did needs no second query. A read
   * after the commit could fail and turn a stored message into a
   * reported error, which invites a resend under a new id.
   */
  toTag: string | null;
  /*
   * Null when the send returned before the policy was read, which
   * an exact retry does. Saying the policy is absent then would be a
   * statement the send never made.
   */
  destinationRequiresTag: boolean | null;
}

export interface RecoveryResult {
  leaseExpired: number;
  requeued: number;
  bounced: number;
  fallbackDemoted: number;
}

export interface LatestMessageState {
  message_id: string;
  status: MessageStatus;
  attempt_id: string | null;
  attempt_count: number;
  presented_at: string | null;
  acked_at: string | null;
}

export interface BridgeStatus {
  message: LatestMessageState & {
    envelope_sha256: string;
    body_sha256: string;
  };
  event_counts: Record<string, number>;
  events: EventRow[];
}

export interface MigrationOptions {
  /**
   * Test-only fault injection used to prove that destructive DDL and all
   * copied rows roll back before schema_version changes.
   */
  failAfterDestructiveDdl?: boolean;
}

export class BridgeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BridgeError";
  }
}

export class BridgeDatabaseError extends BridgeError {
  constructor(message: string) {
    super(message);
    this.name = "BridgeDatabaseError";
  }
}

export class BridgeConflictError extends BridgeError {
  constructor(message: string) {
    super(message);
    this.name = "BridgeConflictError";
  }
}

export class BridgeTransitionError extends BridgeError {
  constructor(
    message: string,
    readonly latest: LatestMessageState | null,
  ) {
    super(message);
    this.name = "BridgeTransitionError";
  }
}

function createMessagesTableSql(tableName: string): string {
  return `
CREATE TABLE ${tableName} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
  root_id TEXT NOT NULL,
  from_role TEXT NOT NULL CHECK (from_role IN ('claude','codex')),
  to_role TEXT NOT NULL CHECK (to_role IN ('claude','codex')),
  to_tag TEXT,
  from_tag TEXT,
  on_timeout TEXT CHECK (
    on_timeout IS NULL OR on_timeout IN ('bounce','fallback')
  ),
  tag_expires_at INTEGER,
  subject TEXT NOT NULL,
  body TEXT NOT NULL,
  envelope_sha256 TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  sender_thread_id TEXT,
  status TEXT NOT NULL DEFAULT 'stored'
    CHECK (
      status IN (
        'stored',
        'claimed',
        'presented',
        'acked',
        'rejected',
        'bounced'
      )
    ),
  attempt_id TEXT,
  consumer TEXT,
  lease_expires_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sent_at TEXT NOT NULL,
  presented_at TEXT,
  acked_at TEXT,
  CHECK (from_role <> to_role),
  CHECK (
    (
      to_tag IS NULL
      AND on_timeout IS NULL
      AND tag_expires_at IS NULL
    )
    OR
    (
      to_tag IS NOT NULL
      AND on_timeout IN ('bounce','fallback')
      AND tag_expires_at IS NOT NULL
    )
  )
);
`;
}

export const SCHEMA_SQL = `
CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

${createMessagesTableSql("messages")}

CREATE INDEX idx_inbox
  ON messages (to_role, status, id);

CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT,
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
`;

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID_RFC_4122 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/*
 * One destination predicate, shared by every place that decides what a
 * session may see. It lived as five copies of the same SQL plus a sixth
 * shape in the hook, which is a machine for making them disagree: the
 * next change to the semantics would have had to find all six.
 *
 * Indented per call site so the surrounding statements stay readable.
 */
export type RolePolicyKey =
  | "require_tag"
  | "strict_addressing";

/*
 * Both policies are the same shape: a set of roles. One parser, so a
 * second copy cannot drift from the first the way the destination
 * predicate did.
 */
export function parseRolePolicy(
  key: RolePolicyKey,
  value: unknown,
): Set<Role> {
  const roles = new Set<Role>();

  if (value === undefined || value === "") {
    return roles;
  }

  if (typeof value !== "string") {
    throw new BridgeError(
      `policy_invalid: ${key} must be text`,
    );
  }

  for (const role of value.split(",")) {
    if (
      role !== "claude" &&
      role !== "codex"
    ) {
      throw new BridgeError(
        `policy_invalid: ${key} must list only claude and codex`,
      );
    }

    roles.add(role);
  }

  return roles;
}

/*
 * One destination predicate, shared by every place that decides what a
 * session may see. It lived as five copies of the same SQL plus a sixth
 * shape in the hook, which is a machine for making them disagree.
 *
 * Under strict addressing the default flips: a session that declared
 * nothing sees nothing, rather than seeing everything unaddressed.
 *
 * Indented per call site so the surrounding statements stay readable.
 */
export function visibleToTagSql(
  indent: string,
  strict: boolean,
): string {
  const lines = strict
    ? [
        "(",
        "  @tag IS NOT NULL",
        "  AND (",
        "    to_tag IS NULL",
        "    OR to_tag = @tag",
        "  )",
        ")",
      ]
    : [
        "(",
        "  to_tag IS NULL",
        "  OR (",
        "    @tag IS NOT NULL",
        "    AND to_tag = @tag",
        "  )",
        ")",
      ];

  return lines
    .map((line) => `${indent}${line}`)
    .join("\n")
    .trimStart();
}

export function getBridgeDbPath(): string {
  const userProfile = process.env.USERPROFILE;
  if (!userProfile) {
    throw new BridgeDatabaseError(
      "USERPROFILE is unavailable; the fixed bridge database path cannot be resolved",
    );
  }

  return join(
    userProfile,
    ".claude",
    "data",
    "agent-bridge",
    "bridge.db",
  );
}

export function oppositeRole(role: Role): Role {
  return role === "claude" ? "codex" : "claude";
}

export function createConsumerId(role: Role): string {
  return `${role}:${process.pid}:${randomUUID()}`;
}

export function sha256(value: string): string {
  return createHash("sha256")
    .update(value, "utf8")
    .digest("hex");
}

export function computeEnvelopeHash(
  fromRole: Role,
  toRole: Role,
  subject: string,
  body: string,
  toTag: string | null = null,
  onTimeout: TimeoutPolicy | null = null,
  fromTag: string | null = null,
): string {
  return sha256(
    JSON.stringify([
      fromRole,
      toRole,
      subject,
      body,
      toTag,
      onTimeout,
      fromTag,
    ]),
  );
}

function normalizeLabel(
  value: unknown,
  field: string,
  maximumBytes: number,
): string {
  if (typeof value !== "string") {
    throw new BridgeError(`${field} must be a string`);
  }

  const normalized = value
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, " ")
    .trim();
  const bytes = Buffer.byteLength(normalized, "utf8");

  if (bytes < 1 || bytes > maximumBytes) {
    throw new BridgeError(
      `${field} must be 1 to ${maximumBytes} UTF-8 bytes after normalization; received ${bytes}`,
    );
  }

  return normalized;
}

export function normalizeSubject(subject: unknown): string {
  return normalizeLabel(subject, "subject", 500);
}

export function normalizeTag(tag: unknown): string {
  return normalizeLabel(tag, "tag", 200);
}

export function validateBody(body: unknown): string {
  if (typeof body !== "string") {
    throw new BridgeError("body must be a string");
  }

  const bytes = Buffer.byteLength(body, "utf8");
  if (bytes < 1 || bytes > 262_144) {
    throw new BridgeError(
      `body must be 1 to 262144 UTF-8 bytes; received ${bytes}`,
    );
  }

  return body;
}

export function validateMessageId(
  messageId: unknown,
): string {
  if (
    typeof messageId !== "string" ||
    !UUID_RFC_4122.test(messageId)
  ) {
    throw new BridgeError(
      "message_id must be an RFC 4122 UUID string",
    );
  }

  return messageId;
}

export function validateAttemptId(
  attemptId: unknown,
): string {
  if (
    typeof attemptId !== "string" ||
    !UUID_V4.test(attemptId)
  ) {
    throw new BridgeError(
      "attempt_id must be a UUIDv4 string",
    );
  }

  return attemptId;
}

function uuidBytes(uuid: string): Buffer {
  return Buffer.from(uuid.replace(/-/g, ""), "hex");
}

function formatUuid(bytes: Buffer): string {
  const hex = bytes.toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

export function deriveBounceMessageId(
  originalMessageIdInput: unknown,
): string {
  const originalMessageId = validateMessageId(
    originalMessageIdInput,
  );
  const namespace = uuidBytes(BOUNCE_NAMESPACE_UUID);
  const digest = createHash("sha1")
    .update(namespace)
    .update(originalMessageId, "utf8")
    .digest();

  const bytes = Buffer.from(digest.subarray(0, 16));
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;

  return formatUuid(bytes);
}

export function initializeBridgeDatabaseAtPath(
  dbPath: string,
  rootId = randomUUID(),
): BridgeMetadata {
  if (!UUID_V4.test(rootId)) {
    throw new BridgeDatabaseError(
      "root_id must be a UUIDv4 string",
    );
  }

  mkdirSync(dirname(dbPath), { recursive: true });

  const db = new Database(dbPath, {
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);

    const journalMode = String(
      db.pragma("journal_mode = WAL", { simple: true }),
    ).toLowerCase();

    if (journalMode !== "wal") {
      throw new BridgeDatabaseError(
        `failed to enable WAL journal mode; received ${journalMode}`,
      );
    }

    const initialize = db.transaction(() => {
      db.exec(SCHEMA_SQL);

      const insertMeta = db.prepare(
        "INSERT INTO meta (k, v) VALUES (?, ?)",
      );
      insertMeta.run("root_id", rootId);
      insertMeta.run("schema_version", SCHEMA_VERSION);
      insertMeta.run(
        "created_at",
        new Date().toISOString(),
      );
    });

    initialize.immediate();

    return {
      dbPath,
      rootId,
      schemaVersion: SCHEMA_VERSION,
    };
  } finally {
    db.close();
  }
}

export function initializeFixedBridgeDatabase(): BridgeMetadata {
  return initializeBridgeDatabaseAtPath(getBridgeDbPath());
}

interface LegacyMessageRow {
  id: number;
  message_id: string;
  root_id: string;
  from_role: Role;
  to_role: Role;
  subject: string;
  body: string;
  envelope_sha256: string;
  body_sha256: string;
  sender_thread_id: string | null;
  status:
    | "stored"
    | "claimed"
    | "presented"
    | "acked"
    | "rejected";
  attempt_id: string | null;
  consumer: string | null;
  lease_expires_at: number | null;
  attempt_count: number;
  sent_at: string;
  presented_at: string | null;
  acked_at: string | null;
}

export function migrateBridgeDatabaseAtPath(
  dbPath: string,
  options: MigrationOptions = {},
): BridgeMetadata {
  if (!existsSync(dbPath)) {
    throw new BridgeDatabaseError(
      `bridge database does not exist: ${dbPath}; initialize version ${LEGACY_SCHEMA_VERSION} before migrating`,
    );
  }

  const db = new Database(dbPath, {
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);

    const integrity = String(
      db.pragma("integrity_check", { simple: true }),
    );
    if (integrity !== "ok") {
      throw new BridgeDatabaseError(
        `PRAGMA integrity_check failed before migration: ${integrity}`,
      );
    }

    const migrate = db.transaction((): BridgeMetadata => {
      const getMeta = db.prepare(
        "SELECT v FROM meta WHERE k = ?",
      );
      const schema = getMeta.get("schema_version") as
        | { v: string }
        | undefined;
      const root = getMeta.get("root_id") as
        | { v: string }
        | undefined;

      if (schema?.v !== LEGACY_SCHEMA_VERSION) {
        throw new BridgeDatabaseError(
          `migration requires schema_version ${LEGACY_SCHEMA_VERSION}; received ${schema?.v ?? "missing"}`,
        );
      }

      if (!root?.v) {
        throw new BridgeDatabaseError(
          "meta.root_id is missing",
        );
      }

      db.exec(createMessagesTableSql("messages_v4"));

      const legacyRows = db
        .prepare("SELECT * FROM messages ORDER BY id")
        .all() as LegacyMessageRow[];

      const insert = db.prepare(
        `INSERT INTO messages_v4 (
           id,
           message_id,
           root_id,
           from_role,
           to_role,
           to_tag,
           from_tag,
           on_timeout,
           tag_expires_at,
           subject,
           body,
           envelope_sha256,
           body_sha256,
           sender_thread_id,
           status,
           attempt_id,
           consumer,
           lease_expires_at,
           attempt_count,
           sent_at,
           presented_at,
           acked_at
         ) VALUES (
           @id,
           @messageId,
           @rootId,
           @fromRole,
           @toRole,
           NULL,
           NULL,
           NULL,
           NULL,
           @subject,
           @body,
           @envelopeHash,
           @bodyHash,
           @senderThreadId,
           @status,
           @attemptId,
           @consumer,
           @leaseExpiresAt,
           @attemptCount,
           @sentAt,
           @presentedAt,
           @ackedAt
         )`,
      );

      for (const row of legacyRows) {
        insert.run({
          id: row.id,
          messageId: row.message_id,
          rootId: row.root_id,
          fromRole: row.from_role,
          toRole: row.to_role,
          subject: row.subject,
          body: row.body,
          envelopeHash: computeEnvelopeHash(
            row.from_role,
            row.to_role,
            row.subject,
            row.body,
            null,
            null,
            null,
          ),
          bodyHash: row.body_sha256,
          senderThreadId: row.sender_thread_id,
          status: row.status,
          attemptId: row.attempt_id,
          consumer: row.consumer,
          leaseExpiresAt: row.lease_expires_at,
          attemptCount: row.attempt_count,
          sentAt: row.sent_at,
          presentedAt: row.presented_at,
          ackedAt: row.acked_at,
        });
      }

      const copiedCount = (
        db
          .prepare(
            "SELECT COUNT(*) AS count FROM messages_v4",
          )
          .get() as { count: number }
      ).count;

      if (copiedCount !== legacyRows.length) {
        throw new BridgeDatabaseError(
          `migration row-count mismatch: source=${legacyRows.length} copied=${copiedCount}`,
        );
      }

      db.exec(`
DROP TABLE messages;
ALTER TABLE messages_v4 RENAME TO messages;
CREATE INDEX idx_inbox
  ON messages (to_role, status, id);
`);

      if (options.failAfterDestructiveDdl) {
        throw new BridgeDatabaseError(
          "injected migration failure after destructive DDL",
        );
      }

      const updateVersion = db
        .prepare(
          `UPDATE meta
              SET v = ?
            WHERE k = 'schema_version'
              AND v = ?`,
        )
        .run(SCHEMA_VERSION, LEGACY_SCHEMA_VERSION);

      if (updateVersion.changes !== 1) {
        throw new BridgeDatabaseError(
          "schema_version changed during migration",
        );
      }

      return {
        dbPath,
        rootId: root.v,
        schemaVersion: SCHEMA_VERSION,
      };
    });

    return migrate.immediate();
  } catch (error) {
    if (error instanceof BridgeDatabaseError) {
      throw error;
    }

    const detail =
      error instanceof Error ? error.message : String(error);
    throw new BridgeDatabaseError(
      `bridge migration failed: ${detail}`,
    );
  } finally {
    db.close();
  }
}

export function migrateFixedBridgeDatabase(
  options: MigrationOptions = {},
): BridgeMetadata {
  return migrateBridgeDatabaseAtPath(
    getBridgeDbPath(),
    options,
  );
}

function openVerifiedDatabase(
  dbPath: string,
  readonly: boolean,
): {
  db: Database.Database;
  metadata: BridgeMetadata;
} {
  if (!existsSync(dbPath)) {
    throw new BridgeDatabaseError(
      `bridge database does not exist: ${dbPath}; run bridge-init first`,
    );
  }

  const db = new Database(dbPath, {
    readonly,
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);

    const integrity = String(
      db.pragma("integrity_check", { simple: true }),
    );
    if (integrity !== "ok") {
      throw new BridgeDatabaseError(
        `PRAGMA integrity_check failed: ${integrity}`,
      );
    }

    const getMeta = db.prepare(
      "SELECT v FROM meta WHERE k = ?",
    );
    const root = getMeta.get("root_id") as
      | { v: string }
      | undefined;
    const schema = getMeta.get("schema_version") as
      | { v: string }
      | undefined;

    if (!root?.v) {
      throw new BridgeDatabaseError(
        "meta.root_id is missing",
      );
    }

    if (!schema?.v) {
      throw new BridgeDatabaseError(
        "meta.schema_version is missing",
      );
    }

    if (schema.v !== SCHEMA_VERSION) {
      throw new BridgeDatabaseError(
        `unsupported schema_version ${schema.v}; expected ${SCHEMA_VERSION}`,
      );
    }

    return {
      db,
      metadata: {
        dbPath,
        rootId: root.v,
        schemaVersion: schema.v,
      },
    };
  } catch (error) {
    db.close();

    if (error instanceof BridgeDatabaseError) {
      throw error;
    }

    const detail =
      error instanceof Error ? error.message : String(error);
    throw new BridgeDatabaseError(
      `bridge database verification failed: ${detail}`,
    );
  }
}

function toIso(epochMs: number): string {
  return new Date(epochMs).toISOString();
}

function latestState(
  row: MessageRow | undefined,
): LatestMessageState | null {
  if (!row) {
    return null;
  }

  return {
    message_id: row.message_id,
    status: row.status,
    attempt_id: row.attempt_id,
    attempt_count: row.attempt_count,
    presented_at: row.presented_at,
    acked_at: row.acked_at,
  };
}

function requireRole(role: unknown): Role {
  if (role !== "claude" && role !== "codex") {
    throw new BridgeError(
      "role must be claude or codex",
    );
  }

  return role;
}

function requireLimit(limit: unknown): number {
  if (
    typeof limit !== "number" ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > MAX_FETCH_LIMIT
  ) {
    throw new BridgeError(
      `limit must be an integer from 1 to ${MAX_FETCH_LIMIT}`,
    );
  }

  return limit;
}

function requireConsumer(consumer: unknown): string {
  if (
    typeof consumer !== "string" ||
    consumer.length === 0
  ) {
    throw new BridgeError(
      "consumer must be a non-empty string",
    );
  }

  return consumer;
}

function optionalTag(value: unknown): string | null {
  if (value === undefined || value === null) {
    return null;
  }

  return normalizeTag(value);
}

function timeoutPolicy(
  value: unknown,
  toTag: string | null,
): TimeoutPolicy | null {
  if (toTag === null) {
    if (value !== undefined && value !== null) {
      throw new BridgeError(
        "on_timeout requires to_tag",
      );
    }

    return null;
  }

  if (value === undefined || value === null) {
    return "bounce";
  }

  if (value !== "bounce" && value !== "fallback") {
    throw new BridgeError(
      "on_timeout must be bounce or fallback",
    );
  }

  return value;
}

export class BridgeBus {
  readonly metadata: BridgeMetadata;
  private closed = false;

  private constructor(
    readonly dbPath: string,
    private readonly db: Database.Database,
    metadata: BridgeMetadata,
  ) {
    this.metadata = metadata;
  }

  static open(dbPath = getBridgeDbPath()): BridgeBus {
    const opened = openVerifiedDatabase(
      dbPath,
      false,
    );
    return new BridgeBus(
      dbPath,
      opened.db,
      opened.metadata,
    );
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.db.close();
  }

  setRolePolicy(
    key: RolePolicyKey,
    value: string,
  ): void {
    /*
     * Validate before writing. Storing a value that cannot be parsed
     * would leave every later call failing, and the command that caused
     * it has already exited.
     */
    parseRolePolicy(key, value);

    this.db
      .prepare(
        `INSERT INTO meta (k, v)
         VALUES (?, ?)
         ON CONFLICT(k)
         DO UPDATE SET v = excluded.v`,
      )
      .run(key, value);
  }

  policyRoles(
    key: RolePolicyKey,
  ): Set<Role> {
    return this.readPolicyRoles(key);
  }

  private readPolicyRoles(
    key: RolePolicyKey,
  ): Set<Role> {
    const row = this.db
      .prepare(
        "SELECT v FROM meta WHERE k = ?",
      )
      .get(key) as
      | { v: unknown }
      | undefined;

    return parseRolePolicy(key, row?.v);
  }

  private strictFor(role: Role): boolean {
    return this.readPolicyRoles(
      "strict_addressing",
    ).has(role);
  }

  send(input: {
    fromRole: Role;
    toRole: Role;
    subject: unknown;
    body: unknown;
    messageId?: unknown;
    senderThreadId?: unknown;
    toTag?: unknown;
    broadcast?: unknown;
    fromTag?: unknown;
    onTimeout?: unknown;
    now?: number;
  }): SendResult {
    const fromRole = requireRole(input.fromRole);
    const toRole = requireRole(input.toRole);

    if (fromRole === toRole) {
      throw new BridgeError(
        "from_role and to_role must differ",
      );
    }

    const subject = normalizeSubject(input.subject);
    const body = validateBody(input.body);
    const toTag = optionalTag(input.toTag);
    const fromTag = optionalTag(input.fromTag);
    const onTimeout = timeoutPolicy(
      input.onTimeout,
      toTag,
    );

    const broadcast =
      input.broadcast === undefined ||
      input.broadcast === null
        ? false
        : input.broadcast;

    if (typeof broadcast !== "boolean") {
      throw new BridgeError(
        "broadcast must be a boolean",
      );
    }

    if (toTag !== null && broadcast) {
      throw new BridgeError(
        "conflicting_destination: to_tag and broadcast cannot both address one message",
      );
    }

    const messageId =
      input.messageId === undefined
        ? randomUUID()
        : validateMessageId(input.messageId);

    let senderThreadId: string | null = null;
    if (
      input.senderThreadId !== undefined &&
      input.senderThreadId !== null
    ) {
      if (
        typeof input.senderThreadId !== "string"
      ) {
        throw new BridgeError(
          "thread_id must be a string when provided",
        );
      }

      senderThreadId = input.senderThreadId;
    }

    const envelopeHash = computeEnvelopeHash(
      fromRole,
      toRole,
      subject,
      body,
      toTag,
      onTimeout,
      fromTag,
    );
    const bodyHash = sha256(body);
    const now = input.now ?? Date.now();
    const sentAt = toIso(now);
    const tagExpiresAt =
      toTag === null ? null : now + TAG_TTL_MS;

    let destinationRequiresTag:
      | boolean
      | null = null;

    type TransactionResult =
      | { kind: "inserted" }
      | { kind: "idempotent" }
      | {
          kind: "conflict";
          existingHash: string;
        };

    const operation = this.db.transaction(
      (): TransactionResult => {
        const existing = this.db
          .prepare(
            `SELECT envelope_sha256
               FROM messages
              WHERE message_id = ?`,
          )
          .get(messageId) as
          | { envelope_sha256: string }
          | undefined;

        if (
          existing &&
          existing.envelope_sha256 === envelopeHash
        ) {
          return { kind: "idempotent" };
        }

        /*
         * Ordered after the idempotent check on purpose. An exact
         * retry of a message that is already stored creates no new
         * delivery, and the turn-head rule tells senders to retry with
         * the same id when a response goes missing. Refusing that
         * because the policy changed in between would push the sender
         * toward a new id, which is a duplicate. Anything that would
         * create or alter a delivery still passes through here.
         *
         * The policy is read here rather than at open, so enabling it
         * reaches servers that are already running. The read shares
         * this transaction with the insert: a database that cannot
         * answer it cannot store the message either, so a failed read
         * has no path that ends in a delivered message. Never catch it
         * into a default.
         */
        const requiredRoles =
          this.readPolicyRoles("require_tag");

        destinationRequiresTag =
          requiredRoles.has(toRole);

        if (
          requiredRoles.has(toRole) &&
          toTag === null &&
          !broadcast
        ) {
          throw new BridgeError(
            "tag_required: address it with to_tag, or set broadcast: true to mean the whole role",
          );
        }

        /*
         * A bounce travels toward the sender's own role and inherits
         * from_tag as its destination, so an undeclared sender leaves
         * the notice of non-delivery role-wide. Either role being under
         * the policy is enough to refuse: the sender's role because
         * that is where the bounce lands, and the destination role
         * because a deployment that demands addressing on the way out
         * should not accept a message whose failure report cannot be
         * addressed. With no policy at all, the role-wide bounce is
         * documented behaviour and stays.
         */
        if (
          (requiredRoles.has(toRole) ||
            requiredRoles.has(fromRole)) &&
          toTag !== null &&
          onTimeout === "bounce" &&
          fromTag === null
        ) {
          throw new BridgeError(
            "sender_tag_required: declare this session with bridge_hello first, or a bounce for this message would arrive role-wide",
          );
        }

        if (existing) {
          this.insertEvent(
            messageId,
            null,
            "send_conflict",
            sentAt,
            JSON.stringify({
              existing_envelope_sha256:
                existing.envelope_sha256,
              attempted_envelope_sha256:
                envelopeHash,
            }),
          );

          return {
            kind: "conflict",
            existingHash:
              existing.envelope_sha256,
          };
        }

        this.db
          .prepare(
            `INSERT INTO messages (
               message_id,
               root_id,
               from_role,
               to_role,
               to_tag,
               from_tag,
               on_timeout,
               tag_expires_at,
               subject,
               body,
               envelope_sha256,
               body_sha256,
               sender_thread_id,
               status,
               sent_at
             ) VALUES (
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               'stored',
               ?
             )`,
          )
          .run(
            messageId,
            this.metadata.rootId,
            fromRole,
            toRole,
            toTag,
            fromTag,
            onTimeout,
            tagExpiresAt,
            subject,
            body,
            envelopeHash,
            bodyHash,
            senderThreadId,
            sentAt,
          );

        this.insertEvent(
          messageId,
          null,
          "sent",
          sentAt,
          null,
        );

        return { kind: "inserted" };
      },
    );

    const result = operation.immediate();

    if (result.kind === "conflict") {
      throw new BridgeConflictError(
        `message_id ${messageId} already exists with a different envelope`,
      );
    }

    return {
      messageId,
      subject,
      idempotent:
        result.kind === "idempotent",
      toTag,
      destinationRequiresTag,
    };
  }

  recover(
    roleInput: Role,
    now = Date.now(),
  ): RecoveryResult {
    const role = requireRole(roleInput);
    const operation = this.db.transaction(() =>
      this.recoverWithinTransaction(role, now),
    );
    return operation.immediate();
  }

  private recoverWithinTransaction(
    role: Role,
    now: number,
  ): RecoveryResult {
    let bounced = 0;
    let fallbackDemoted = 0;
    const nowIso = toIso(now);
    const presentedCutoff = toIso(
      now - PRESENTED_TTL_MS,
    );

    const expiredClaims = this.db
      .prepare(
        `SELECT id, message_id, attempt_id
           FROM messages
          WHERE to_role = ?
            AND status = 'claimed'
            AND lease_expires_at < ?
          ORDER BY id`,
      )
      .all(role, now) as Array<{
      id: number;
      message_id: string;
      attempt_id: string | null;
    }>;

    for (const row of expiredClaims) {
      const update = this.db
        .prepare(
          `UPDATE messages
              SET status = 'stored',
                  attempt_id = NULL,
                  consumer = NULL,
                  lease_expires_at = NULL
            WHERE id = ?
              AND to_role = ?
              AND status = 'claimed'
              AND lease_expires_at < ?`,
        )
        .run(row.id, role, now);

      this.assertOneChange(
        update.changes,
        `claimed->stored recovery failed for ${row.message_id}`,
      );

      this.insertEvent(
        row.message_id,
        row.attempt_id,
        "lease_expired",
        nowIso,
        JSON.stringify({
          recovered_by_role: role,
        }),
      );
    }

    const stalePresented = this.db
      .prepare(
        `SELECT id, message_id, attempt_id
           FROM messages
          WHERE to_role = ?
            AND status = 'presented'
            AND acked_at IS NULL
            AND presented_at < ?
          ORDER BY id`,
      )
      .all(role, presentedCutoff) as Array<{
      id: number;
      message_id: string;
      attempt_id: string | null;
    }>;

    for (const row of stalePresented) {
      const update = this.db
        .prepare(
          `UPDATE messages
              SET status = 'stored',
                  attempt_id = NULL,
                  consumer = NULL,
                  lease_expires_at = NULL
            WHERE id = ?
              AND to_role = ?
              AND status = 'presented'
              AND acked_at IS NULL
              AND presented_at < ?`,
        )
        .run(
          row.id,
          role,
          presentedCutoff,
        );

      this.assertOneChange(
        update.changes,
        `presented->stored recovery failed for ${row.message_id}`,
      );

      this.insertEvent(
        row.message_id,
        row.attempt_id,
        "requeued",
        nowIso,
        JSON.stringify({
          recovered_by_role: role,
        }),
      );
    }

    /*
     * This third stage intentionally runs after both recovery stages and
     * inside their transaction. It therefore includes rows returned from
     * claimed or presented to stored during this sweep.
     */
    const expiredTagged = this.db
      .prepare(
        `SELECT *
           FROM messages
          WHERE to_role = ?
            AND status = 'stored'
            AND to_tag IS NOT NULL
            AND tag_expires_at < ?
          ORDER BY id`,
      )
      .all(role, now) as MessageRow[];

    for (const row of expiredTagged) {
      if (row.on_timeout === "fallback") {
        const update = this.db
          .prepare(
            `UPDATE messages
                SET to_tag = NULL,
                    on_timeout = NULL,
                    tag_expires_at = NULL
              WHERE id = ?
                AND to_role = ?
                AND status = 'stored'
                AND to_tag IS NOT NULL
                AND on_timeout = 'fallback'
                AND tag_expires_at < ?`,
          )
          .run(row.id, role, now);

        this.assertOneChange(
          update.changes,
          `tag fallback failed for ${row.message_id}`,
        );

        this.insertEvent(
          row.message_id,
          null,
          "tag_fallback",
          nowIso,
          null,
        );
        fallbackDemoted += 1;
        continue;
      }

      if (row.on_timeout !== "bounce") {
        throw new BridgeTransitionError(
          `expired tagged row has invalid on_timeout for ${row.message_id}`,
          latestState(row),
        );
      }

      const update = this.db
        .prepare(
          `UPDATE messages
              SET status = 'bounced'
            WHERE id = ?
              AND to_role = ?
              AND status = 'stored'
              AND to_tag IS NOT NULL
              AND on_timeout = 'bounce'
              AND tag_expires_at < ?`,
        )
        .run(row.id, role, now);

      this.assertOneChange(
        update.changes,
        `stored->bounced failed for ${row.message_id}`,
      );

      const bounceMessageId =
        deriveBounceMessageId(row.message_id);
      const bounceBody =
        `${BOUNCE_REASON}; ` +
        `original_message_id=${row.message_id}`;
      const bounceToTag = row.from_tag;
      const bounceOnTimeout: TimeoutPolicy | null =
        bounceToTag === null ? null : "fallback";
      const bounceTagExpiresAt =
        bounceToTag === null
          ? null
          : now + TAG_TTL_MS;
      const bounceEnvelopeHash =
        computeEnvelopeHash(
          row.to_role,
          row.from_role,
          BOUNCE_SUBJECT,
          bounceBody,
          bounceToTag,
          bounceOnTimeout,
          null,
        );

      const existingBounce = this.db
        .prepare(
          `SELECT envelope_sha256
             FROM messages
            WHERE message_id = ?`,
        )
        .get(bounceMessageId) as
        | { envelope_sha256: string }
        | undefined;

      let insertedBounce = false;
      if (existingBounce) {
        if (
          existingBounce.envelope_sha256 !==
          bounceEnvelopeHash
        ) {
          throw new BridgeConflictError(
            `bounce message_id ${bounceMessageId} already exists with a different envelope`,
          );
        }
      } else {
        this.db
          .prepare(
            `INSERT INTO messages (
               message_id,
               root_id,
               from_role,
               to_role,
               to_tag,
               from_tag,
               on_timeout,
               tag_expires_at,
               subject,
               body,
               envelope_sha256,
               body_sha256,
               sender_thread_id,
               status,
               sent_at
             ) VALUES (
               ?,
               ?,
               ?,
               ?,
               ?,
               NULL,
               ?,
               ?,
               ?,
               ?,
               ?,
               ?,
               NULL,
               'stored',
               ?
             )`,
          )
          .run(
            bounceMessageId,
            this.metadata.rootId,
            row.to_role,
            row.from_role,
            bounceToTag,
            bounceOnTimeout,
            bounceTagExpiresAt,
            BOUNCE_SUBJECT,
            bounceBody,
            bounceEnvelopeHash,
            sha256(bounceBody),
            nowIso,
          );
        insertedBounce = true;
      }

      this.insertEvent(
        row.message_id,
        null,
        "bounced",
        nowIso,
        JSON.stringify({
          bounce_message_id: bounceMessageId,
        }),
      );

      if (insertedBounce) {
        this.insertEvent(
          bounceMessageId,
          null,
          "sent",
          nowIso,
          null,
        );
      }

      bounced += 1;
    }

    return {
      leaseExpired: expiredClaims.length,
      requeued: stalePresented.length,
      bounced,
      fallbackDemoted,
    };
  }

  claim(
    roleInput: Role,
    consumerInput: string,
    limitInput = DEFAULT_FETCH_LIMIT,
    now = Date.now(),
    sessionTagInput: unknown = null,
  ): ClaimedMessage[] {
    const role = requireRole(roleInput);
    const consumer = requireConsumer(consumerInput);
    const limit = requireLimit(limitInput);
    const sessionTag = optionalTag(sessionTagInput);

    /*
     * The policy is read inside the transaction that claims, so
     * enabling it cannot land between the read and the select. A
     * default of false here would leave the invariant with the caller
     * rather than the transition.
     */
    const operation = this.db.transaction(() =>
      this.claimWithinTransaction(
        role,
        consumer,
        limit,
        now,
        sessionTag,
        null,
        this.strictFor(role),
      ),
    );

    return operation.immediate();
  }

  private claimWithinTransaction(
    role: Role,
    consumer: string,
    limit: number,
    now: number,
    sessionTag: string | null,
    messageId: string | null = null,
    strict = false,
  ): ClaimedMessage[] {
    const claimedAt = toIso(now);
    const leaseExpiresAt = now + CLAIM_LEASE_MS;

    const rows = this.db
      .prepare(
        `SELECT *
           FROM messages
          WHERE to_role = @role
            AND status = 'stored'
            AND (
              @messageId IS NULL
              OR message_id = @messageId
            )
            AND ${visibleToTagSql(
              "            ",
              strict,
            )}
          ORDER BY id
          LIMIT @limit`,
      )
      .all({
        role,
        tag: sessionTag,
        limit,
        messageId,
      }) as MessageRow[];

    const claimed: ClaimedMessage[] = [];

    for (const row of rows) {
      const attemptId = randomUUID();

      const update = this.db
        .prepare(
          `UPDATE messages
              SET status = 'claimed',
                  attempt_id = @attemptId,
                  consumer = @consumer,
                  lease_expires_at = @leaseExpiresAt,
                  attempt_count = attempt_count + 1,
                  presented_at = NULL
            WHERE id = @id
              AND status = 'stored'
              AND to_role = @role
              AND (
                @messageId IS NULL
                OR message_id = @messageId
              )
              AND ${visibleToTagSql(
                "              ",
                strict,
              )}`,
        )
        .run({
          attemptId,
          consumer,
          leaseExpiresAt,
          id: row.id,
          role,
          tag: sessionTag,
          messageId,
        });

      this.assertOneChange(
        update.changes,
        `stored->claimed failed for ${row.message_id}`,
      );

      this.insertEvent(
        row.message_id,
        attemptId,
        "claimed",
        claimedAt,
        JSON.stringify({ consumer }),
      );

      const rejectionReasons: string[] = [];
      if (
        row.root_id !== this.metadata.rootId
      ) {
        rejectionReasons.push(
          "root_id mismatch",
        );
      }

      if (
        sha256(row.body) !== row.body_sha256
      ) {
        rejectionReasons.push(
          "body_sha256 mismatch",
        );
      }

      if (rejectionReasons.length > 0) {
        const rejection = this.db
          .prepare(
            `UPDATE messages
                SET status = 'rejected'
              WHERE id = ?
                AND to_role = ?
                AND status = 'claimed'
                AND attempt_id = ?
                AND consumer = ?`,
          )
          .run(
            row.id,
            role,
            attemptId,
            consumer,
          );

        this.assertOneChange(
          rejection.changes,
          `claimed->rejected failed for ${row.message_id}`,
        );

        this.insertEvent(
          row.message_id,
          attemptId,
          "rejected",
          claimedAt,
          rejectionReasons.join("; "),
        );

        continue;
      }

      const attemptCount =
        row.attempt_count + 1;
      claimed.push({
        ...row,
        status: "claimed",
        attempt_id: attemptId,
        consumer,
        lease_expires_at: leaseExpiresAt,
        attempt_count: attemptCount,
        presented_at: null,
        redelivery: attemptCount > 1,
      });
    }

    return claimed;
  }

  markPresented(
    roleInput: Role,
    consumerInput: string,
    messages: ReadonlyArray<{
      messageId: string;
      attemptId: string;
    }>,
    now = Date.now(),
  ): void {
    if (messages.length === 0) {
      return;
    }

    const role = requireRole(roleInput);
    const consumer = requireConsumer(consumerInput);
    const presentedAt = toIso(now);

    const operation = this.db.transaction(() => {
      for (const message of messages) {
        const messageId = validateMessageId(
          message.messageId,
        );
        const attemptId = validateAttemptId(
          message.attemptId,
        );

        const update = this.db
          .prepare(
            `UPDATE messages
                SET status = 'presented',
                    presented_at = ?
              WHERE message_id = ?
                AND to_role = ?
                AND status = 'claimed'
                AND attempt_id = ?
                AND consumer = ?`,
          )
          .run(
            presentedAt,
            messageId,
            role,
            attemptId,
            consumer,
          );

        this.assertOneChange(
          update.changes,
          `claimed->presented failed for ${messageId}`,
        );

        this.insertEvent(
          messageId,
          attemptId,
          "presented",
          presentedAt,
          JSON.stringify({ consumer }),
        );
      }
    });

    operation.immediate();
  }

  ack(
    roleInput: Role,
    messageIdInput: unknown,
    attemptIdInput: unknown,
    now = Date.now(),
    consumerInput: unknown = undefined,
  ): LatestMessageState {
    const role = requireRole(roleInput);
    const consumer = requireConsumer(
      consumerInput,
    );
    const messageId = validateMessageId(
      messageIdInput,
    );
    const attemptId = validateAttemptId(
      attemptIdInput,
    );
    const ackedAt = toIso(now);

    type AckResult =
      | {
          ok: true;
          state: LatestMessageState;
        }
      | {
          ok: false;
          latest: LatestMessageState | null;
        };

    const operation = this.db.transaction(
      (): AckResult => {
        const update = this.db
          .prepare(
            /*
             * The consumer condition is what makes an attempt_id stop
             * being a credential. It leaks from bridge_status, from the
             * events in that same reply, and from a failed ack, which
             * returns the current state. Hiding it is not available;
             * requiring that the acking process is the one the message
             * was presented to is. markPresented has always required
             * this, and ack not requiring it was the asymmetry.
             */
            `UPDATE messages
                SET status = 'acked',
                    acked_at = @ackedAt
              WHERE message_id = @messageId
                AND to_role = @role
                AND status = 'presented'
                AND attempt_id = @attemptId
                AND consumer = @consumer`,
          )
          .run({
            ackedAt,
            messageId,
            role,
            attemptId,
            consumer,
          });

        if (update.changes === 0) {
          const row = this.readMessage(
            messageId,
          );
          return {
            ok: false,
            latest: latestState(row),
          };
        }

        this.assertOneChange(
          update.changes,
          `presented->acked changed an unexpected number of rows for ${messageId}`,
        );

        this.insertEvent(
          messageId,
          attemptId,
          "acked",
          ackedAt,
          null,
        );

        const row = this.readMessage(
          messageId,
        );
        if (!row) {
          throw new BridgeTransitionError(
            `acked row disappeared for ${messageId}`,
            null,
          );
        }

        return {
          ok: true,
          state: latestState(row)!,
        };
      },
    );

    const result = operation.immediate();

    if (!result.ok) {
      throw new BridgeTransitionError(
        `bridge_ack rejected for ${messageId}: this process is not the one the message is currently presented to under attempt ${attemptId} for role ${role}. A message presented to a server process that has since restarted returns to the queue after the presented TTL and is offered again.`,
        result.latest,
      );
    }

    return result.state;
  }

  fetch(
    roleInput: Role,
    consumerInput: string,
    options: {
      peek?: boolean;
      limit?: number;
      now?: number;
      tag?: unknown;
      messageId?: unknown;
    } = {},
  ): FetchResult {
    const role = requireRole(roleInput);
    const consumer = requireConsumer(
      consumerInput,
    );
    const peek = options.peek ?? false;
    const messageId =
      options.messageId === undefined ||
      options.messageId === null
        ? null
        : validateMessageId(
            options.messageId,
          );

    /*
     * A message_id fetch takes exactly one row, so limit stops
     * applying. Clients that fill the schema default would otherwise
     * have to omit it.
     */
    const limit =
      messageId === null
        ? requireLimit(
            options.limit ??
              DEFAULT_FETCH_LIMIT,
          )
        : 1;
    const now = options.now ?? Date.now();
    const sessionTag = optionalTag(options.tag);

    if (peek) {
      return this.peek(
        role,
        limit,
        sessionTag,
        messageId,
        this.strictFor(role),
      );
    }

    /*
     * Recovery stages and claim selection share one BEGIN IMMEDIATE
     * transaction. A row recovered from claimed or presented therefore
     * reaches tag timeout before any session can re-claim it.
     */
    const recoverAndClaim = this.db.transaction(
      (): {
        claimed: ClaimedMessage[];
        strict: boolean;
      } => {
        this.recoverWithinTransaction(
          role,
          now,
        );

        /*
         * Read inside the transaction that claims. Reading before it
         * lets an enable land in between, and the counts below have to
         * answer under the same value the select used.
         */
        const strict = this.strictFor(role);

        return {
          strict,
          claimed:
            this.claimWithinTransaction(
              role,
              consumer,
              limit,
              now,
              sessionTag,
              messageId,
              strict,
            ),
        };
      },
    );

    const { claimed, strict } =
      recoverAndClaim.immediate();

    this.markPresented(
      role,
      consumer,
      claimed.map((message) => ({
        messageId: message.message_id,
        attemptId: message.attempt_id,
      })),
      now,
    );

    return {
      declared_tag: sessionTag,
      messages: claimed.map((message) => ({
        message_id: message.message_id,
        attempt_id: message.attempt_id,
        subject: message.subject,
        to_tag: message.to_tag,
        from_tag: message.from_tag,
        body_bytes: Buffer.byteLength(
          message.body,
          "utf8",
        ),
        body: message.body,
        redelivery: message.redelivery,
      })),
      has_more:
        this.countStored(
          this.db,
          role,
          sessionTag,
          strict,
        ) > 0,
      unacked_total: this.countUnacked(
        this.db,
        role,
        sessionTag,
        strict,
      ),
      peek: false,
    };
  }

  status(
    messageIdInput: unknown,
  ): BridgeStatus {
    const messageId = validateMessageId(
      messageIdInput,
    );
    const row = this.readMessage(messageId);

    if (!row) {
      throw new BridgeError(
        `message_id not found: ${messageId}`,
      );
    }

    const events = this.db
      .prepare(
        `SELECT
           seq,
           message_id,
           attempt_id,
           event,
           at,
           detail
         FROM events
         WHERE message_id = ?
         ORDER BY seq`,
      )
      .all(messageId) as EventRow[];

    const eventCounts: Record<
      string,
      number
    > = {};
    for (const event of events) {
      eventCounts[event.event] =
        (eventCounts[event.event] ?? 0) + 1;
    }

    return {
      message: {
        ...latestState(row)!,
        envelope_sha256:
          row.envelope_sha256,
        body_sha256: row.body_sha256,
      },
      event_counts: eventCounts,
      events,
    };
  }

  readMessage(
    messageIdInput: unknown,
  ): MessageRow | undefined {
    const messageId = validateMessageId(
      messageIdInput,
    );
    return this.db
      .prepare(
        "SELECT * FROM messages WHERE message_id = ?",
      )
      .get(messageId) as
      | MessageRow
      | undefined;
  }

  private peek(
    role: Role,
    limit: number,
    sessionTag: string | null,
    messageId: string | null = null,
    strict = false,
  ): FetchResult {
    const opened = openVerifiedDatabase(
      this.dbPath,
      true,
    );

    try {
      const rows = opened.db
        .prepare(
          `SELECT *
             FROM messages
            WHERE to_role = @role
              AND status = 'stored'
              AND (
                @messageId IS NULL
                OR message_id = @messageId
              )
              AND ${visibleToTagSql(
                "              ",
                strict,
              )}
            ORDER BY id
            LIMIT @limit`,
        )
        .all({
          role,
          tag: sessionTag,
          limit,
          messageId,
        }) as MessageRow[];

      return {
        declared_tag: sessionTag,
        messages: rows.map((message) => ({
          message_id: message.message_id,
          attempt_id: message.attempt_id,
          subject: message.subject,
          to_tag: message.to_tag,
          from_tag: message.from_tag,
          body_bytes: Buffer.byteLength(
            message.body,
            "utf8",
          ),
          redelivery:
            message.attempt_count > 0,
        })),
        has_more:
          this.countStored(
            opened.db,
            role,
            sessionTag,
            strict,
          ) > rows.length,
        unacked_total: this.countUnacked(
          opened.db,
          role,
          sessionTag,
          strict,
        ),
        peek: true,
      };
    } finally {
      opened.db.close();
    }
  }

  private countStored(
    db: Database.Database,
    role: Role,
    sessionTag: string | null,
    strict: boolean,
  ): number {
    const row = db
      .prepare(
        `SELECT COUNT(*) AS count
           FROM messages
          WHERE to_role = @role
            AND status = 'stored'
            AND ${visibleToTagSql(
              "            ",
              strict,
            )}`,
      )
      .get({
        role,
        tag: sessionTag,
      }) as { count: number };

    return row.count;
  }

  private countUnacked(
    db: Database.Database,
    role: Role,
    sessionTag: string | null,
    strict: boolean,
  ): number {
    const row = db
      .prepare(
        `SELECT COUNT(*) AS count
           FROM messages
          WHERE to_role = @role
            AND status IN (
              'stored',
              'claimed',
              'presented'
            )
            AND ${visibleToTagSql(
              "            ",
              strict,
            )}`,
      )
      .get({
        role,
        tag: sessionTag,
      }) as { count: number };

    return row.count;
  }

  private insertEvent(
    messageId: string | null,
    attemptId: string | null,
    event: string,
    at: string,
    detail: string | null,
  ): void {
    this.db
      .prepare(
        `INSERT INTO events (
           message_id,
           attempt_id,
           event,
           at,
           detail
         ) VALUES (?, ?, ?, ?, ?)`,
      )
      .run(
        messageId,
        attemptId,
        event,
        at,
        detail,
      );
  }

  private assertOneChange(
    changes: number,
    message: string,
  ): void {
    if (changes !== 1) {
      throw new BridgeTransitionError(
        message,
        null,
      );
    }
  }
}
