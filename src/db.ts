import { createHash, randomUUID } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  unlinkSync,
} from "node:fs";
import { basename, dirname, join } from "node:path";
import Database from "better-sqlite3";
import { quoteForOneLine, writeErrorRecord } from "./one-line.js";

export const LEGACY_SCHEMA_VERSION = "3.2";
export const SCHEMA_VERSION = "4.13";
export const MIGRATION_LOCK_KEY = "migration_in_progress";
export const MIGRATION_PAUSE_ENV = "AGENT_BRIDGE_PAUSE_AFTER_DESTRUCTIVE_DDL";

/*
 * The versions a migration knows how to walk, oldest first. `--migrate`
 * held one fixed source version, so a third version meant editing the
 * same four places again; a pair per row means the next one is a row.
 *
 * A step carries its own shape: the staging table and the DDL that builds
 * it, the table it replaces, how the rows move, and the indexes to put
 * back. The executor holds the order and nothing else, which is what lets
 * a version that only adds a table be another row here (`ddl`) instead of
 * another branch there.
 *
 * The copy has two arms because one cannot express both. 3.2 has no
 * addressing columns, so its rows are rebuilt field by field and the
 * envelope recomputed over the seven elements, which is not a statement.
 * 4.0 already carries every column and only the CHECK widens, so `SELECT
 * *` copies it and `envelope_sha256` cannot move.
 */
export type MigrationCopy =
  | { via: "sql"; sql: string }
  | {
      via: "rows";
      rows: (
        db: Database.Database,
        staging: string,
        options: MigrationOptions,
      ) => void;
    };

export interface RebuildMigrationStep {
  kind: "rebuild";
  from: string;
  to: string;
  table: string;
  staging: string;
  stagingSql: string;
  copy: MigrationCopy;
  after: readonly string[];
}

export interface DdlMigrationStep {
  kind: "ddl";
  from: string;
  to: string;
  statements: readonly string[];
}

export interface FillMigrationStep {
  kind: "fill";
  from: string;
  to: string;
  rows: (db: Database.Database, options: MigrationOptions) => void;
}

export type MigrationStep =
  RebuildMigrationStep | DdlMigrationStep | FillMigrationStep;
export const BUSY_TIMEOUT_MS = 5_000;
export const CLAIM_LEASE_MS = 120_000;
export const PRESENTED_TTL_MS = 15 * 60_000;
export const TAG_TTL_MS = 30 * 60_000;
export const DEFAULT_FETCH_LIMIT = 3;
export const MAX_FETCH_LIMIT = 10;

export const BOUNCE_NAMESPACE_UUID = "2fce6f02-4d78-4e23-9e04-a04e565f7c72";
export const BOUNCE_SUBJECT = "bridge: undelivered";
export const BOUNCE_REASON = "destination session tag expired before delivery";

export type Role = "claude" | "codex";

export interface EndpointMappingEndpoint {
  role: Role;
  name: string;
}

export interface EndpointMappingTag {
  role: Role;
  tag: string | null;
  endpoint: string;
}

export interface EndpointMapping {
  endpoints: readonly EndpointMappingEndpoint[];
  tags: readonly EndpointMappingTag[];
}

export interface MigrationLock {
  pid: number;
  started_at: string;
}

export type TimeoutPolicy = "bounce" | "fallback";
export type MessageStatus =
  "stored" | "claimed" | "presented" | "acked" | "rejected" | "bounced";

export interface BridgeMetadata {
  dbPath: string;
  rootId: string;
  schemaVersion: string;
}

export interface MigrationMetadata extends BridgeMetadata {
  backupPath: string;
}

export interface MessageRow {
  id: number;
  message_id: string;
  from_role: Role;
  to_role: Role;
  to_tag: string | null;
  from_tag: string | null;
  on_timeout: TimeoutPolicy | null;
  tag_expires_at: number | null;
  subject: string;
  body: string;
  envelope_sha256: string;
  envelope_version: number;
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
  source_endpoint_id: string | null;
  legacy_to_tag: string | null;
}

export interface EndpointRow {
  endpoint_id: string;
  role: Role;
  name: string;
  created_at: string;
  retired_at: string | null;
}

export interface EventRow {
  seq: number;
  message_id: string | null;
  attempt_id: string | null;
  event: string;
  at: string;
  detail: string | null;
  endpoint?: string | null;
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
  from_endpoint?: string | null;
  to_tag?: string | null;
  from_tag?: string | null;
  body_bytes: number;
  body?: string;
  redelivery: boolean;
}

export interface FetchResult {
  declared_tag?: string | null;
  /*
   * Peek changes nothing, so repeating it returns the same rows. A
   * session that leaves a page for someone else steps past it with
   * this; null means there is nothing after what was just returned.
   */
  next_cursor?: number | null;
  messages: FetchMessage[];
  has_more: boolean;
  unacked_total: number;
  /*
   * Rows only the sweep can move. unacked_total counts live claims and
   * presentations too, so an empty page beside a non-zero total is an
   * ordinary delivery in flight elsewhere as often as it is a backlog.
   * Without this the reader has to guess which, and the rule guessed.
   */
  recovery_owed?: number;
  peek: boolean;
}

export interface StoredSendResult {
  messageId: string;
  subject: string;
  idempotent: boolean;
  /*
   * Both are decided inside the send transaction and returned, so a
   * caller describing what it just did needs no second query.
   */
  toTag: string | null;
  /*
   * Null when an exact retry returns before the policy is read.
   */
  destinationRequiresTag: boolean | null;
  /** Endpoint names inserted by this call. Empty on an exact retry. */
  added?: string[];
}

/*
 * Stage four allows a message to reach several endpoints, so the refusal
 * "second_delivery_before_stage4" has no producer any more and the
 * union collapsed to the stored result.
 */
export type SendResult = StoredSendResult;

export interface RecoveryResult {
  leaseExpired: number;
  requeued: number;
  bounced: number;
  fallbackDemoted: number;
}

export interface BacklogCounts {
  stuck: number;
  oldestSentAt: string | null;
}

export interface BacklogRow {
  from_tag?: string | null;
  from_endpoint?: string | null;
  sent_at: string;
}

/*
 * A cursor per role. Sharing one let a bounce landing between the two
 * queries advance it past a loss the first role had already been asked
 * about and reported nothing for, and that loss can never satisfy
 * `seq > cursor` again.
 */
function sweepCursorKey(role: Role): string {
  return `sweep_scan_cursor_${role}`;
}

/*
 * One definition, so the page and the count cannot answer about
 * different rows, and so a test can ask the planner about the statement
 * that actually runs rather than a copy of it.
 *
 * The bound is plain rather than `@since IS NULL OR e.seq > @since`. The
 * nullable form stopped SQLite seeking on the rowid and the plan read
 * `SCAN e`, which a sweep every thirty minutes pays for over an events
 * table nothing prunes. Sequences start at 1, so zero means everything
 * and the branch is not needed.
 */
export function lostQuerySql(): {
  page: string;
  count: string;
} {
  /*
   * Join the bounced event to its own delivery. A message_id join
   * duplicates or drops a page once one message has two deliveries.
   * e.seq stays a bare bound so the events primary key is still a seek.
   */
  const window = `
           FROM messages m
           JOIN message_events e
             ON e.message_id = m.message_id
            AND e.event = 'bounced'
           JOIN deliveries d
             ON d.delivery_id = e.delivery_id
           JOIN endpoints dead
             ON dead.endpoint_id = d.endpoint_id
           LEFT JOIN endpoints src
             ON src.endpoint_id = m.source_endpoint_id
          WHERE dead.role = @role
            AND d.state = 'bounced'
            AND e.seq > @since`;

  return {
    page: `SELECT m.subject AS subject,
                src.name  AS bounceTo,
                dead.name AS deadEndpoint,
                e.at      AS at,
                e.seq     AS seq
         ${window}
          ORDER BY e.seq
          LIMIT @limit`,
    count: `SELECT COUNT(*) AS count ${window}`,
  };
}

export interface UndeliveredMessage {
  subject: string;
  /** Where the bounce went: the sender's lane, or null if it went role-wide. */
  bounceToTag: string | null;
  /** The address that did not answer. Not a place to go looking for the row. */
  deadTag: string | null;
  bounceTo?: string | null;
  deadEndpoint?: string | null;
  at: string;
  /*
   * The event's own sequence, which is what the caller pages by. Every
   * message a single sweep bounces carries the same wall-clock stamp, so
   * a cursor on the timestamp either repeats the whole batch or steps
   * over it. There is no third option, and the batch is the normal case.
   */
  seq: number;
}

export interface UndeliveredReport {
  /*
   * Deliveries that failed since the previous sweep, oldest first and no
   * more than the caller asked for. Oldest first because the caller pages
   * forward and stops at a cap: newest first drops the oldest, and a
   * cursor that only moves forward never comes back for them.
   */
  lost: UndeliveredMessage[];
  /* How many failed in that window, so a capped page can say what it left. */
  lostSince: number;
  /* Every failed delivery, so the total stays visible after its line scrolls past. */
  lostTotal: number;
}

export interface LatestMessageState {
  message_id: string;
  status: MessageStatus | "cancelled";
  attempt_id: string | null;
  attempt_count: number;
  presented_at: string | null;
  acked_at: string | null;
}

export interface BridgeStatus {
  message_id?: string;
  legacy_to_tag?: string | null;
  legacy_from_tag?: string | null;
  envelope_sha256?: string;
  body_sha256?: string;
  deliveries?: Array<{
    endpoint: string;
    state: string;
    holder: string | null;
    attempt_id: string | null;
    attempt_count: number;
    lease_until: number | null;
    presented_at: string | null;
    confirmed_at: string | null;
  }>;
  message?: LatestMessageState & {
    envelope_sha256: string;
    body_sha256: string;
  };
  event_counts: Record<string, number>;
  events: EventRow[];
  unacked_total?: number;
  recovery_owed?: number;
}

export interface MigrationOptions {
  /**
   * Test-only fault injection used to prove that destructive DDL and all
   * copied rows roll back before schema_version changes.
   */
  failAfterDestructiveDdl?: boolean;
  /**
   * Test-only process pause. The CLI exposes this only through
   * MIGRATION_PAUSE_ENV so a child can be terminated at the DDL seam.
   */
  pauseAfterDestructiveDdl?: boolean;
  /**
   * Read by the 4.10 fill and the 4.13 message copy.
   * Absent fails those steps, and fails the 4.10 cutover guard.
   */
  mapping?: EndpointMapping;
  /**
   * Precheck 2a and 2b. Absent is 未確認, and a 4.10 cutover refuses.
   */
  configPaths?: readonly string[];
  /**
   * Test seam. Walk only until this version. bridge-init does not set it.
   */
  stopAt?: string;
  /**
   * b-3 applies one step without the six cutover checks.
   * bridge-init does not set it.
   */
  skipCutoverChecks?: boolean;
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/*
 * The one rule for an endpoint name, used by addEndpoint and by the
 * mapping validation, so a name the mapping accepts is a name
 * --add-endpoint accepts (Codex review of PR #42). Refused rather than
 * repaired: resolveEndpoint compares the --endpoint argument as it
 * arrives, so a stored name that differs from the typed one is a row no
 * server can select. Control characters, U+2028 and U+2029 would also
 * break the one-line records bridge-init and the server write.
 */
export function endpointNameProblem(endpointName: string): string | null {
  if (endpointName.trim().length === 0) {
    return "endpoint name must be a non-empty string";
  }

  const nameBytes = Buffer.byteLength(endpointName, "utf8");
  if (nameBytes > 200) {
    return `endpoint name is ${nameBytes} UTF-8 bytes; register a name of 200 bytes or fewer`;
  }

  if (/[\u0000-\u001f\u007f-\u009f\u2028\u2029]/.test(endpointName)) {
    return `endpoint name ${quoteForOneLine(
      endpointName,
    )} holds a control character; register a name that prints as the one line it is written on`;
  }

  if (endpointName !== endpointName.trim()) {
    return `endpoint name ${quoteForOneLine(
      endpointName,
    )} is padded with whitespace; register the name exactly as --endpoint will be given it`;
  }

  return null;
}

function mappingShapeError(detail: string): never {
  throw new BridgeDatabaseError(`mapping shape is invalid: ${detail}`);
}

export function validateEndpointMapping(value: unknown): EndpointMapping {
  if (
    !isRecord(value) ||
    !Array.isArray(value.endpoints) ||
    !Array.isArray(value.tags)
  ) {
    return mappingShapeError("expected endpoints[] and tags[]");
  }

  const endpoints: EndpointMappingEndpoint[] = [];
  const endpointKeys = new Set<string>();

  for (const [index, candidate] of value.endpoints.entries()) {
    if (
      !isRecord(candidate) ||
      (candidate.role !== "claude" && candidate.role !== "codex") ||
      typeof candidate.name !== "string" ||
      candidate.name.trim().length === 0
    ) {
      return mappingShapeError(
        `endpoints[${index}] must contain role=claude|codex and a non-empty name`,
      );
    }

    const nameProblem = endpointNameProblem(candidate.name);
    if (nameProblem !== null) {
      return mappingShapeError(`endpoints[${index}] ${nameProblem}`);
    }

    const key = `${candidate.role}\u0000${candidate.name}`;
    if (endpointKeys.has(key)) {
      return mappingShapeError(`endpoints[${index}] duplicates role/name`);
    }

    endpointKeys.add(key);
    endpoints.push({
      role: candidate.role,
      name: candidate.name,
    });
  }

  const tags: EndpointMappingTag[] = [];
  const tagKeys = new Set<string>();

  for (const [index, candidate] of value.tags.entries()) {
    if (
      !isRecord(candidate) ||
      (candidate.role !== "claude" && candidate.role !== "codex") ||
      !(
        candidate.tag === null ||
        (typeof candidate.tag === "string" && candidate.tag.trim().length > 0)
      ) ||
      typeof candidate.endpoint !== "string" ||
      candidate.endpoint.trim().length === 0
    ) {
      return mappingShapeError(
        `tags[${index}] must contain role=claude|codex, tag=string|null, and a non-empty endpoint`,
      );
    }

    const tagKey = `${candidate.role}\u0000${
      candidate.tag === null ? "\u0000default" : candidate.tag
    }`;
    if (tagKeys.has(tagKey)) {
      return mappingShapeError(`tags[${index}] duplicates a role/tag mapping`);
    }
    tagKeys.add(tagKey);

    const sameRole = endpoints.some(
      (endpoint) =>
        endpoint.role === candidate.role &&
        endpoint.name === candidate.endpoint,
    );

    if (!sameRole) {
      const anotherRole = endpoints.some(
        (endpoint) => endpoint.name === candidate.endpoint,
      );

      if (anotherRole) {
        throw new BridgeDatabaseError(
          `mapping tag endpoint role does not match: role=${candidate.role} endpoint=${JSON.stringify(
            candidate.endpoint,
          )}`,
        );
      }

      throw new BridgeDatabaseError(
        `mapping tag endpoint is not declared: role=${candidate.role} endpoint=${JSON.stringify(
          candidate.endpoint,
        )}`,
      );
    }

    tags.push({
      role: candidate.role,
      tag: candidate.tag,
      endpoint: candidate.endpoint,
    });
  }

  return { endpoints, tags };
}

function readMigrationLock(db: Database.Database): MigrationLock | null {
  const row = db
    .prepare("SELECT v FROM meta WHERE k = ?")
    .get(MIGRATION_LOCK_KEY) as { v: unknown } | undefined;

  if (!row) {
    return null;
  }

  if (typeof row.v !== "string") {
    throw new BridgeDatabaseError(
      "meta.migration_in_progress is invalid; restore from the backup",
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(row.v);
  } catch {
    throw new BridgeDatabaseError(
      "meta.migration_in_progress is invalid; restore from the backup",
    );
  }

  if (
    !isRecord(parsed) ||
    typeof parsed.pid !== "number" ||
    !Number.isInteger(parsed.pid) ||
    parsed.pid <= 0 ||
    typeof parsed.started_at !== "string" ||
    parsed.started_at.length === 0 ||
    Number.isNaN(Date.parse(parsed.started_at))
  ) {
    throw new BridgeDatabaseError(
      "meta.migration_in_progress is invalid; restore from the backup",
    );
  }

  return {
    pid: parsed.pid,
    started_at: parsed.started_at,
  };
}

export function formatMigrationLock(lock: MigrationLock): string {
  return `migration in progress since ${lock.started_at} pid=${lock.pid}`;
}

export function readMigrationLockAtPath(dbPath: string): MigrationLock | null {
  if (!existsSync(dbPath)) {
    return null;
  }

  const db = new Database(dbPath, {
    readonly: true,
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);
    return readMigrationLock(db);
  } finally {
    db.close();
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

function createMessagesTableSql410(
  tableName: string,
  includeEnvelopeVersion = true,
): string {
  return `
CREATE TABLE ${tableName} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
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
${includeEnvelopeVersion ? "  envelope_version INTEGER NOT NULL,\n" : ""}  body_sha256 TEXT NOT NULL,
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
  source_endpoint_id TEXT REFERENCES endpoints(endpoint_id),
  legacy_to_tag TEXT,
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
      AND on_timeout IS NOT NULL
      AND on_timeout IN ('bounce','fallback')
      AND tag_expires_at IS NOT NULL
    )
    OR
    (
      to_tag IS NOT NULL
      AND on_timeout IS NULL
      AND tag_expires_at IS NULL
    )
  )
);
`;
}

function createMessagesTableSql(tableName: string): string {
  return `
CREATE TABLE ${tableName} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
  from_role TEXT NOT NULL CHECK (from_role IN ('claude','codex')),
  source_endpoint_id TEXT NOT NULL REFERENCES endpoints(endpoint_id),
  legacy_to_tag TEXT,
  legacy_from_tag TEXT,
  subject TEXT NOT NULL,
  body TEXT NOT NULL,
  envelope_sha256 TEXT NOT NULL,
  envelope_version INTEGER NOT NULL,
  body_sha256 TEXT NOT NULL,
  sender_thread_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sent_at TEXT NOT NULL
);
`;
}

const ENDPOINTS_TABLE_SQL = `
CREATE TABLE endpoints (
  endpoint_id TEXT PRIMARY KEY,
  role TEXT NOT NULL CHECK (role IN ('claude','codex')),
  name TEXT NOT NULL,
  created_at TEXT NOT NULL,
  retired_at TEXT,
  UNIQUE (role, name)
);
`;

const DELIVERIES_TABLE_SQL_4_6 = `
CREATE TABLE deliveries (
  delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL REFERENCES messages(message_id),
  endpoint_id TEXT NOT NULL REFERENCES endpoints(endpoint_id),
  state TEXT NOT NULL CHECK (state IN
    ('pending','leased','presented','confirmed','rejected','bounced','cancelled')),
  holder TEXT,
  attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  presented_at TEXT,
  confirmed_at TEXT,
  UNIQUE (message_id, endpoint_id)
);
`;

const DELIVERIES_TABLE_SQL_4_7 = `
CREATE TABLE deliveries (
  delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL REFERENCES messages(message_id),
  endpoint_id TEXT REFERENCES endpoints(endpoint_id),
  state TEXT NOT NULL CHECK (state IN
    ('pending','leased','presented','confirmed','rejected','bounced','cancelled')),
  holder TEXT,
  attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  presented_at TEXT,
  confirmed_at TEXT,
  UNIQUE (message_id, endpoint_id),
  CHECK (attempt_count >= 0),
  CHECK (
    (
      state = 'pending'
      AND holder IS NULL
      AND attempt_id IS NULL
      AND lease_until IS NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'leased'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NOT NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'presented'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NULL
      AND presented_at IS NOT NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'confirmed'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NULL
      AND presented_at IS NOT NULL
      AND confirmed_at IS NOT NULL
    )
    OR
    (
      state = 'rejected'
      AND lease_until IS NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state IN ('bounced','cancelled')
      AND lease_until IS NULL
      AND confirmed_at IS NULL
    )
  )
);
`;

const DELIVERIES_ONE_PER_MESSAGE_INDEX_SQL = `
CREATE UNIQUE INDEX deliveries_one_per_message
  ON deliveries (message_id);
`;

function createEventsTableSql(tableName: string): string {
  return `
CREATE TABLE ${tableName} (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id INTEGER NOT NULL REFERENCES deliveries(delivery_id),
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
`;
}

const MESSAGE_EVENTS_VIEW_SQL = `
CREATE VIEW message_events AS
SELECT d.message_id, d.endpoint_id, e.*
  FROM events e
  JOIN deliveries d USING (delivery_id);
`;

const ENDPOINTS_IMMUTABLE_TRIGGER_SQL = `
CREATE TRIGGER endpoints_immutable BEFORE UPDATE OF role, name ON endpoints
BEGIN SELECT RAISE(ABORT, 'endpoint role/name are immutable'); END;
`;

const DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL = `
CREATE TRIGGER deliveries_role_differs BEFORE INSERT ON deliveries
BEGIN
  SELECT RAISE(ABORT, 'delivery to the sender role')
   WHERE (SELECT role FROM endpoints WHERE endpoint_id = NEW.endpoint_id)
       = (SELECT from_role FROM messages WHERE message_id = NEW.message_id);
END;
`;

const DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL = `
CREATE TRIGGER deliveries_role_differs_on_assign
BEFORE UPDATE OF endpoint_id ON deliveries
WHEN NEW.endpoint_id IS NOT NULL
BEGIN
  SELECT RAISE(ABORT, 'delivery to the sender role')
   WHERE (SELECT role FROM endpoints WHERE endpoint_id = NEW.endpoint_id)
       = (SELECT from_role FROM messages WHERE message_id = NEW.message_id);
END;
`;

const DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL_4_6 = `
CREATE TRIGGER deliveries_identity_immutable
BEFORE UPDATE OF message_id, endpoint_id ON deliveries
BEGIN SELECT RAISE(ABORT, 'delivery message/endpoint are immutable'); END;
`;

const DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL = `
CREATE TRIGGER deliveries_identity_immutable
BEFORE UPDATE OF message_id, endpoint_id ON deliveries
WHEN OLD.endpoint_id IS NOT NULL
  OR NEW.message_id <> OLD.message_id
BEGIN SELECT RAISE(ABORT, 'delivery message/endpoint are immutable'); END;
`;

const MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL_4_8 = `
CREATE TRIGGER messages_identity_immutable
BEFORE UPDATE OF
  message_id,
  from_role,
  source_endpoint_id,
  legacy_to_tag,
  subject,
  body,
  envelope_sha256,
  envelope_version
ON messages
BEGIN SELECT RAISE(ABORT, 'message identity is immutable'); END;
`;

const MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL = `
CREATE TRIGGER messages_identity_immutable
BEFORE UPDATE OF
  message_id,
  from_role,
  source_endpoint_id,
  legacy_to_tag,
  legacy_from_tag,
  subject,
  body,
  envelope_sha256,
  envelope_version
ON messages
BEGIN SELECT RAISE(ABORT, 'message identity is immutable'); END;
`;

/*
 * A table and the trigger that guards it, in the two groups the ladder
 * puts on either side of the rebuild. Held here rather than spelled into
 * the steps so a test standing in a wrong implementation builds on the
 * same statements the real one runs.
 */
export const STAGE_ONE_ENDPOINTS_SQL: readonly string[] = [
  ENDPOINTS_TABLE_SQL,
  ENDPOINTS_IMMUTABLE_TRIGGER_SQL,
];

export const STAGE_ONE_DELIVERIES_SQL: readonly string[] = [
  DELIVERIES_TABLE_SQL_4_6,
  DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
  DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL_4_6,
];

function createDeliveriesTableSql(tableName: string): string {
  return `
CREATE TABLE ${tableName} (
  delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL REFERENCES messages(message_id),
  endpoint_id TEXT NOT NULL REFERENCES endpoints(endpoint_id),
  state TEXT NOT NULL CHECK (state IN
    ('pending','leased','presented','confirmed','rejected','bounced','cancelled')),
  holder TEXT,
  attempt_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  presented_at TEXT,
  confirmed_at TEXT,
  UNIQUE (message_id, endpoint_id),
  CHECK (attempt_count >= 0),
  CHECK (
    (
      state = 'pending'
      AND holder IS NULL
      AND attempt_id IS NULL
      AND lease_until IS NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'leased'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NOT NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'presented'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NULL
      AND presented_at IS NOT NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state = 'confirmed'
      AND holder IS NOT NULL
      AND attempt_id IS NOT NULL
      AND lease_until IS NULL
      AND presented_at IS NOT NULL
      AND confirmed_at IS NOT NULL
    )
    OR
    (
      state = 'rejected'
      AND lease_until IS NULL
      AND presented_at IS NULL
      AND confirmed_at IS NULL
    )
    OR
    (
      state IN ('bounced','cancelled')
      AND lease_until IS NULL
      AND confirmed_at IS NULL
    )
  )
);
`;
}

const DELIVERIES_ENDPOINT_STATE_INDEX_SQL = `
CREATE INDEX idx_deliveries_endpoint_state
  ON deliveries (endpoint_id, state, delivery_id);
`;

export const SCHEMA_SQL = `
CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
${ENDPOINTS_TABLE_SQL}
${createMessagesTableSql("messages")}
${createDeliveriesTableSql("deliveries")}
${createEventsTableSql("events")}
${MESSAGE_EVENTS_VIEW_SQL}
${ENDPOINTS_IMMUTABLE_TRIGGER_SQL}${DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL}${DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL}${DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL}${MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL}
${DELIVERIES_ENDPOINT_STATE_INDEX_SQL}`;

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID_RFC_4122 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function assertRootId(value: unknown, where: "root_id" | "meta.root_id"): void {
  /*
   * The type check comes first: a BLOB holding the bytes of a valid UUID
   * comes back as a Buffer, and RegExp.test would stringify and accept it.
   */
  if (typeof value !== "string" || !UUID_V4.test(value)) {
    throw new BridgeDatabaseError(
      where === "root_id"
        ? "root_id must be a UUIDv4 string"
        : `${where} is not a UUIDv4: ${
            typeof value === "string"
              ? quoteForOneLine(value)
              : `<${typeof value}>`
          }`,
    );
  }
}

export type RolePolicyKey = "require_tag" | "strict_addressing";

/*
 * Both policies are the same shape: a set of roles. One parser, so a
 * second copy cannot drift from the first the way the destination
 * predicate did.
 */
export function parseRolePolicy(key: RolePolicyKey, value: unknown): Set<Role> {
  const roles = new Set<Role>();

  if (value === undefined || value === "") {
    return roles;
  }

  if (typeof value !== "string") {
    throw new BridgeError(`policy_invalid: ${key} must be text`);
  }

  for (const role of value.split(",")) {
    if (role !== "claude" && role !== "codex") {
      throw new BridgeError(
        `policy_invalid: ${key} must list only claude and codex`,
      );
    }

    roles.add(role);
  }

  return roles;
}

export function getBridgeDbPath(): string {
  const userProfile = process.env.USERPROFILE;
  if (!userProfile) {
    throw new BridgeDatabaseError(
      "USERPROFILE is unavailable; the fixed bridge database path cannot be resolved",
    );
  }

  return join(userProfile, ".claude", "data", "agent-bridge", "bridge.db");
}

export function oppositeRole(role: Role): Role {
  return role === "claude" ? "codex" : "claude";
}

export function createConsumerId(role: Role): string {
  return `${role}:${process.pid}:${randomUUID()}`;
}

export function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function computeLegacyEnvelopeHash(
  fromRole: Role,
  toRole: Role,
  subject: string,
  body: string,
  toTag: string | null,
  onTimeout: TimeoutPolicy | null,
  fromTag: string | null,
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

export function computeEnvelopeHash(
  fromRole: Role,
  subject: string,
  body: string,
): string {
  return sha256(JSON.stringify([2, fromRole, subject, body, null, null, 0]));
}

/*
 * This seam exists so a test can prove that send, bounce, and the 4.8
 * migration row copy share one formula. Production code never reassigns it.
 */
export const envelopeHashSeam = {
  compute: computeEnvelopeHash,
};

function normalizeLabel(
  value: unknown,
  field: string,
  maximumBytes: number,
): string {
  if (typeof value !== "string") {
    throw new BridgeError(`${field} must be a string`);
  }

  const normalized = value.replace(/[\u0000-\u001f\u007f-\u009f]/g, " ").trim();
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

/*
 * The one place a lane can name itself ahead of time. The hook runs in
 * its own process and cannot see what bridge_hello told the server, so
 * the lane says it in the environment of the settings file that
 * registered the hook. The MCP server for that same session is started
 * from the same environment, which is why this lives here rather than
 * beside the hook: both sides read the same variable, and the server is
 * the only one of them in a position to notice that the value and the
 * declaration disagree.
 */
export const DECLARED_TAG_ENV = "AGENT_BRIDGE_TAG";

export interface DeclaredTag {
  /** The address this process answers to, or null if it named none. */
  tag: string | null;
  /*
   * Why the environment could not be used, when it was set to something
   * that is not a tag. Separate from `tag` being null on purpose: unset
   * and unusable are different states, and collapsing them is what let a
   * misconfigured variable read as a deliberate silence.
   */
  unusable: string | null;
}

/*
 * Never throws. Unset means no address, which costs least when it is
 * wrong: an undeclared lane is not told about mail it could have taken,
 * rather than every lane being told about mail it cannot.
 *
 * A value that is not a tag lands in the same place, and says so. It
 * used to throw, and the hook's catch-all turned that into a stderr line
 * and exit 0 -- so a typo in one settings file silenced every notice
 * that hook had, including the untagged mail the tag has nothing to do
 * with. Unset failed safe and misconfigured failed dark; there was no
 * reason for the two to differ, and the darker one was the one a person
 * could cause by hand.
 */
export function readDeclaredTag(
  env: NodeJS.ProcessEnv = process.env,
): DeclaredTag {
  const raw = env[DECLARED_TAG_ENV];

  if (raw === undefined || raw.trim().length === 0) {
    return { tag: null, unusable: null };
  }

  try {
    return {
      tag: normalizeTag(raw),
      unusable: null,
    };
  } catch (error) {
    return {
      tag: null,
      unusable: `${DECLARED_TAG_ENV} is not a usable tag: ${
        error instanceof Error ? error.message : String(error)
      }`,
    };
  }
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

export function validateMessageId(messageId: unknown): string {
  if (typeof messageId !== "string" || !UUID_RFC_4122.test(messageId)) {
    throw new BridgeError("message_id must be an RFC 4122 UUID string");
  }

  return messageId;
}

export function validateAttemptId(attemptId: unknown): string {
  if (typeof attemptId !== "string" || !UUID_V4.test(attemptId)) {
    throw new BridgeError("attempt_id must be a UUIDv4 string");
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

export function deriveBounceMessageId(originalMessageIdInput: unknown): string {
  const originalMessageId = validateMessageId(originalMessageIdInput);
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
  assertRootId(rootId, "root_id");

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

      const insertMeta = db.prepare("INSERT INTO meta (k, v) VALUES (?, ?)");
      insertMeta.run("root_id", rootId);
      insertMeta.run("schema_version", SCHEMA_VERSION);
      insertMeta.run("created_at", new Date().toISOString());
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
  status: "stored" | "claimed" | "presented" | "acked" | "rejected";
  attempt_id: string | null;
  consumer: string | null;
  lease_expires_at: number | null;
  attempt_count: number;
  sent_at: string;
  presented_at: string | null;
  acked_at: string | null;
}

/*
 * Every step from where this database is to where the build is, or an
 * error naming both. Walking the table instead of comparing against one
 * constant means a database two versions behind is migrated rather than
 * told it is unsupported, and it never comes to rest on a version no
 * server will open.
 */
export function planMigration(
  from: string,
  steps: readonly MigrationStep[] = MIGRATION_STEPS,
  target: string = SCHEMA_VERSION,
): MigrationStep[] {
  const planned: MigrationStep[] = [];
  let at = from;

  while (at !== target) {
    const step = steps.find((candidate) => candidate.from === at);

    if (!step) {
      throw new BridgeDatabaseError(
        `no migration path from schema_version ${at} to ${target}; the versions that can be migrated from are ${steps
          .map((candidate) => candidate.from)
          .join(", ")}`,
      );
    }

    planned.push(step);
    at = step.to;
  }

  return planned;
}

/*
 * Staged under a name of its own, so a failure leaves `messages` as the
 * only table by that name rather than a half-built second one.
 */
const MIGRATION_STAGING_TABLE = "messages_next";

function copyLegacyRows(db: Database.Database, staging: string): void {
  const legacyRows = db
    .prepare("SELECT * FROM messages ORDER BY id")
    .all() as LegacyMessageRow[];

  const insert = db.prepare(
    `INSERT INTO ${staging} (
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
      envelopeHash: computeLegacyEnvelopeHash(
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
}

const MESSAGES_INBOX_INDEX_SQL = `
CREATE INDEX idx_inbox
  ON messages (to_role, status, id);
`;

/*
 * Positional, and that is the point: the steps that carry this build their
 * staging from the frozen 4.2 SQL and read a table already at that shape,
 * so the columns line up and every value including `envelope_sha256`
 * arrives unchanged. Naming them here would be a second list to keep in
 * step with the first. A step whose staging drops a column cannot use it;
 * `COPY_WITHOUT_ROOT_ID` below names both sides for that reason.
 */
const COPY_EVERY_COLUMN: MigrationCopy = {
  via: "sql",
  sql: `INSERT INTO ${MIGRATION_STAGING_TABLE} SELECT * FROM messages;`,
};

/*
 * A copy of the table as it stood at 4.2, frozen here rather than read off
 * `createMessagesTableSql`. The steps below it end on versions that are
 * already in the field, so what they build has to stay what they built:
 * once the helper moved to 4.3 a shared call would have retired `root_id`
 * from the 3.2 staging table too, and `copyLegacyRows` names that column
 * in its insert.
 */
const MESSAGES_STAGING_SQL_4_2 = `
CREATE TABLE ${MIGRATION_STAGING_TABLE} (
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
      AND on_timeout IS NOT NULL
      AND on_timeout IN ('bounce','fallback')
      AND tag_expires_at IS NOT NULL
    )
    OR
    (
      to_tag IS NOT NULL
      AND on_timeout IS NULL
      AND tag_expires_at IS NULL
    )
  )
);
`;

const MESSAGES_COLUMNS_4_3 = `
  id,
  message_id,
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
  acked_at`;

/*
 * Named on both sides, unlike the positional copy above, and for opposite
 * reasons on either side of 4.3. Going in, the source still has `root_id`
 * and the staging table does not; coming out, the staging table has the
 * two endpoint columns and the source does not. `SELECT *` would load
 * every value one place off in the first case and refuse the second.
 *
 * Both name the 4.3 columns because that is what a table at 4.3 holds,
 * which is the source of one and the destination of the other.
 */
function copyNamedColumns(columns: string): MigrationCopy {
  return {
    via: "sql",
    sql: `INSERT INTO ${MIGRATION_STAGING_TABLE} (${columns}
) SELECT ${columns}
  FROM messages;`,
  };
}

const COPY_WITHOUT_ROOT_ID: MigrationCopy =
  copyNamedColumns(MESSAGES_COLUMNS_4_3);

const COPY_WITH_NEW_COLUMNS_NULL: MigrationCopy =
  copyNamedColumns(MESSAGES_COLUMNS_4_3);

/*
 * Frozen for the same reason as the 4.2 table above: 4.3 is in the field,
 * so the step that ends there has to keep building what it built once
 * `createMessagesTableSql` moved on past it.
 */
const MESSAGES_STAGING_SQL_4_3 = `
CREATE TABLE ${MIGRATION_STAGING_TABLE} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
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
      AND on_timeout IS NOT NULL
      AND on_timeout IN ('bounce','fallback')
      AND tag_expires_at IS NOT NULL
    )
    OR
    (
      to_tag IS NOT NULL
      AND on_timeout IS NULL
      AND tag_expires_at IS NULL
    )
  )
);
`;

const STAGE_TWO_DELIVERIES_SQL = `
CREATE TEMP TABLE stage_two_delivery_count (
  value INTEGER NOT NULL
);
CREATE TEMP TRIGGER stage_two_deliveries_must_be_empty
BEFORE INSERT ON stage_two_delivery_count
WHEN NEW.value <> 0
BEGIN
  SELECT RAISE(ABORT, 'deliveries must be empty before stage two');
END;
INSERT INTO stage_two_delivery_count
SELECT COUNT(*) FROM deliveries;
DROP TRIGGER stage_two_deliveries_must_be_empty;
DROP TABLE stage_two_delivery_count;
DROP TRIGGER deliveries_role_differs;
DROP TRIGGER deliveries_identity_immutable;
DROP TABLE deliveries;
${DELIVERIES_TABLE_SQL_4_7}
${DELIVERIES_ONE_PER_MESSAGE_INDEX_SQL}
${DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL}
`;

function copyStageTwoRows(db: Database.Database, staging: string): void {
  const rows = db
    .prepare(
      `SELECT id,
              from_role,
              subject,
              body
         FROM messages
        ORDER BY id`,
    )
    .all() as Array<Pick<MessageRow, "id" | "from_role" | "subject" | "body">>;

  const insert = db.prepare(
    `INSERT INTO ${staging} (
       id,
       message_id,
       from_role,
       to_role,
       to_tag,
       from_tag,
       on_timeout,
       tag_expires_at,
       subject,
       body,
       envelope_sha256,
       envelope_version,
       body_sha256,
       sender_thread_id,
       status,
       attempt_id,
       consumer,
       lease_expires_at,
       attempt_count,
       sent_at,
       presented_at,
       acked_at,
       source_endpoint_id,
       legacy_to_tag
     )
     SELECT id,
            message_id,
            from_role,
            to_role,
            to_tag,
            from_tag,
            on_timeout,
            tag_expires_at,
            subject,
            body,
            @envelopeHash,
            2,
            body_sha256,
            sender_thread_id,
            status,
            attempt_id,
            consumer,
            lease_expires_at,
            attempt_count,
            sent_at,
            presented_at,
            acked_at,
            source_endpoint_id,
            to_tag
       FROM messages
      WHERE id = @id`,
  );

  for (const row of rows) {
    insert.run({
      id: row.id,
      envelopeHash: envelopeHashSeam.compute(
        row.from_role,
        row.subject,
        row.body,
      ),
    });
  }
}

function fillDeliveries(db: Database.Database): void {
  const inserted = db
    .prepare(
      `INSERT INTO deliveries (
         message_id,
         endpoint_id,
         state,
         holder,
         attempt_id,
         attempt_count,
         lease_until,
         presented_at,
         confirmed_at
       )
       SELECT message_id,
              NULL,
              CASE status
                WHEN 'stored' THEN 'pending'
                WHEN 'claimed' THEN 'leased'
                WHEN 'presented' THEN 'presented'
                WHEN 'acked' THEN 'confirmed'
                WHEN 'rejected' THEN 'rejected'
                WHEN 'bounced' THEN 'bounced'
              END,
              CASE
                WHEN status IN (
                  'claimed',
                  'presented',
                  'acked',
                  'rejected',
                  'bounced'
                )
                THEN consumer
                ELSE NULL
              END,
              CASE
                WHEN status IN (
                  'claimed',
                  'presented',
                  'acked',
                  'rejected',
                  'bounced'
                )
                THEN attempt_id
                ELSE NULL
              END,
              attempt_count,
              CASE
                WHEN status = 'claimed'
                THEN lease_expires_at
                ELSE NULL
              END,
              CASE
                WHEN status IN (
                  'presented',
                  'acked',
                  'bounced'
                )
                THEN presented_at
                ELSE NULL
              END,
              CASE
                WHEN status = 'acked'
                THEN acked_at
                ELSE NULL
              END
         FROM messages
        ORDER BY id`,
    )
    .run();

  const messages = rowCount(db, "messages");
  if (inserted.changes !== messages) {
    throw new BridgeDatabaseError(
      `delivery fill row-count mismatch: messages=${messages} deliveries=${inserted.changes}`,
    );
  }
}

interface StageFourSourceRow {
  id: number;
  message_id: string;
  from_role: Role;
  from_tag: string | null;
  subject: string;
  body: string;
  envelope_sha256: string;
  envelope_version: number;
  body_sha256: string;
  sender_thread_id: string | null;
  attempt_count: number;
  sent_at: string;
  source_endpoint_id: string | null;
  legacy_to_tag: string | null;
}

function lookupMappedEndpoint(
  db: Database.Database,
  mapping: EndpointMapping,
  role: Role,
  tag: string | null,
  what: "delivery" | "source",
): { endpointId: string; role: Role } {
  const entry = mapping.tags.find(
    (candidate) => candidate.role === role && candidate.tag === tag,
  );

  if (entry === undefined) {
    throw new BridgeDatabaseError(
      tag === null
        ? `migration fill: no default mapping for untagged ${what} role=${role}`
        : `migration fill: no mapping for ${what} tag role=${role} tag=${JSON.stringify(tag)}`,
    );
  }

  const rows = db
    .prepare(
      `SELECT endpoint_id, role
         FROM endpoints
        WHERE name = ?`,
    )
    .all(entry.endpoint) as Array<{
    endpoint_id: string;
    role: Role;
  }>;
  const match = rows.find((row) => row.role === role);

  if (match === undefined) {
    if (rows.length > 0) {
      return {
        endpointId: rows[0]?.endpoint_id ?? "",
        role: rows[0]?.role ?? role,
      };
    }

    throw new BridgeDatabaseError(
      `migration fill: mapping endpoint is not registered role=${role} endpoint=${JSON.stringify(entry.endpoint)}`,
    );
  }

  return {
    endpointId: match.endpoint_id,
    role: match.role,
  };
}

function insertMappingEndpoints(
  db: Database.Database,
  mapping: EndpointMapping,
): void {
  const insert = db.prepare(
    `INSERT INTO endpoints (
       endpoint_id, role, name, created_at, retired_at
     )
     SELECT @endpointId, @role, @name, @createdAt, NULL
      WHERE NOT EXISTS (
        SELECT 1
          FROM endpoints
         WHERE role = @role
           AND name = @name
      )`,
  );
  const createdAt = new Date().toISOString();

  for (const endpoint of mapping.endpoints) {
    insert.run({
      endpointId: randomUUID(),
      role: endpoint.role,
      name: endpoint.name,
      createdAt,
    });
  }
}

function fillDeliveryEndpoints(
  db: Database.Database,
  options: MigrationOptions,
): void {
  if (options.mapping === undefined) {
    throw new BridgeDatabaseError("migration 4.10 to 4.11 requires --mapping");
  }

  insertMappingEndpoints(db, options.mapping);

  const pending = db
    .prepare(
      `SELECT d.delivery_id AS deliveryId,
              m.to_role AS toRole,
              m.from_role AS fromRole,
              m.legacy_to_tag AS tag
         FROM deliveries d
         JOIN messages m
           ON m.message_id = d.message_id
        WHERE d.endpoint_id IS NULL
        ORDER BY d.delivery_id`,
    )
    .all() as Array<{
    deliveryId: number;
    toRole: Role;
    fromRole: Role;
    tag: string | null;
  }>;
  const update = db.prepare(
    `UPDATE deliveries
        SET endpoint_id = @endpointId
      WHERE delivery_id = @deliveryId
        AND endpoint_id IS NULL`,
  );

  for (const row of pending) {
    const found = lookupMappedEndpoint(
      db,
      options.mapping,
      row.toRole,
      row.tag,
      "delivery",
    );

    /*
     * Same role as the sender is not rejected here. The assign trigger
     * aborts that UPDATE. Any other role that is not to_role is ours.
     */
    if (found.role !== row.toRole && found.role !== row.fromRole) {
      throw new BridgeDatabaseError(
        `migration fill: mapping endpoint role differs from to_role role=${row.toRole} endpoint_role=${found.role}`,
      );
    }

    const result = update.run({
      endpointId: found.endpointId,
      deliveryId: row.deliveryId,
    });

    if (result.changes !== 1) {
      throw new BridgeDatabaseError(
        `migration fill: delivery ${row.deliveryId} was not assigned`,
      );
    }
  }
}

function copyStageFourMessages(
  db: Database.Database,
  staging: string,
  options: MigrationOptions,
): void {
  if (options.mapping === undefined) {
    throw new BridgeDatabaseError("migration 4.12 to 4.13 requires --mapping");
  }

  const rows = db
    .prepare(
      `SELECT id,
              message_id,
              from_role,
              from_tag,
              subject,
              body,
              envelope_sha256,
              envelope_version,
              body_sha256,
              sender_thread_id,
              attempt_count,
              sent_at,
              source_endpoint_id,
              legacy_to_tag
         FROM messages
        ORDER BY id`,
    )
    .all() as StageFourSourceRow[];
  const insert = db.prepare(
    `INSERT INTO ${staging} (
       id,
       message_id,
       from_role,
       source_endpoint_id,
       legacy_to_tag,
       legacy_from_tag,
       subject,
       body,
       envelope_sha256,
       envelope_version,
       body_sha256,
       sender_thread_id,
       attempt_count,
       sent_at
     ) VALUES (
       @id,
       @messageId,
       @fromRole,
       @sourceEndpointId,
       @legacyToTag,
       @legacyFromTag,
       @subject,
       @body,
       @envelopeSha256,
       @envelopeVersion,
       @bodySha256,
       @senderThreadId,
       @attemptCount,
       @sentAt
     )`,
  );

  for (const row of rows) {
    let sourceEndpointId = row.source_endpoint_id;

    if (sourceEndpointId === null) {
      const found = lookupMappedEndpoint(
        db,
        options.mapping,
        row.from_role,
        row.from_tag,
        "source",
      );

      if (found.role !== row.from_role) {
        throw new BridgeDatabaseError(
          `migration fill: mapping endpoint role differs from source role=${row.from_role} endpoint_role=${found.role}`,
        );
      }

      sourceEndpointId = found.endpointId;
    }

    insert.run({
      id: row.id,
      messageId: row.message_id,
      fromRole: row.from_role,
      sourceEndpointId,
      legacyToTag: row.legacy_to_tag,
      legacyFromTag: row.from_tag,
      subject: row.subject,
      body: row.body,
      envelopeSha256: row.envelope_sha256,
      envelopeVersion: row.envelope_version,
      bodySha256: row.body_sha256,
      senderThreadId: row.sender_thread_id,
      attemptCount: row.attempt_count,
      sentAt: row.sent_at,
    });
  }
}

function rebuildMessages(
  from: string,
  to: string,
  stagingSql: string,
  copy: MigrationCopy,
  after: readonly string[] = [MESSAGES_INBOX_INDEX_SQL],
): RebuildMigrationStep {
  return {
    kind: "rebuild",
    from,
    to,
    table: "messages",
    staging: MIGRATION_STAGING_TABLE,
    stagingSql,
    copy,
    after,
  };
}

export const MIGRATION_STEPS: readonly MigrationStep[] = [
  rebuildMessages(LEGACY_SCHEMA_VERSION, "4.0", MESSAGES_STAGING_SQL_4_2, {
    via: "rows",
    rows: copyLegacyRows,
  }),
  rebuildMessages("4.0", "4.1", MESSAGES_STAGING_SQL_4_2, COPY_EVERY_COLUMN),
  rebuildMessages("4.1", "4.2", MESSAGES_STAGING_SQL_4_2, COPY_EVERY_COLUMN),
  rebuildMessages("4.2", "4.3", MESSAGES_STAGING_SQL_4_3, COPY_WITHOUT_ROOT_ID),
  /*
   * Two tables, two columns and two triggers are one stage, spread over
   * three versions because `planMigration` takes the first step
   * matching a version and would never reach a second one carrying the
   * same `from`.
   *
   * The order is not free, and the two ends of it fail for unrelated
   * reasons. Put `deliveries` before the rebuild and the rebuild dies
   * on `ALTER TABLE ... RENAME`, which reparses the whole schema:
   * `error in trigger deliveries_role_differs: no such table:
   * main.messages`, the table the `DROP TABLE` one statement earlier
   * took away. Put `endpoints` after the rebuild and the rename is
   * never reached. better-sqlite3 opens every connection with foreign
   * keys on, so the copy into the staging table dies on `no such
   * table: main.endpoints`, the registry the new column references;
   * with foreign keys off that copy and its rename both pass. So the
   * registry goes in ahead of the rebuild and the delivery table
   * follows it.
   *
   * Three versions rather than two is a limit of the step kinds, not
   * of the order: nothing in a rebuild runs after the rename except
   * `indexes`. Two versions would mean handing the rebuild work on the
   * far side of that rename, either the `deliveries` DDL sitting in
   * `indexes` or a `DROP TRIGGER` and a re-`CREATE` around it.
   *
   * A later step that rebuilds `messages` meets the first of those
   * two: it has to drop `deliveries_role_differs` and put it back.
   */
  {
    kind: "ddl",
    from: "4.3",
    to: "4.4",
    statements: STAGE_ONE_ENDPOINTS_SQL,
  },
  rebuildMessages(
    "4.4",
    "4.5",
    createMessagesTableSql410(MIGRATION_STAGING_TABLE, false),
    COPY_WITH_NEW_COLUMNS_NULL,
  ),
  {
    kind: "ddl",
    from: "4.5",
    to: "4.6",
    statements: STAGE_ONE_DELIVERIES_SQL,
  },
  {
    kind: "ddl",
    from: "4.6",
    to: "4.7",
    statements: [STAGE_TWO_DELIVERIES_SQL],
  },
  rebuildMessages(
    "4.7",
    "4.8",
    createMessagesTableSql410(MIGRATION_STAGING_TABLE),
    {
      via: "rows",
      rows: copyStageTwoRows,
    },
    [
      MESSAGES_INBOX_INDEX_SQL,
      DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
      DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL,
      MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL_4_8,
    ],
  ),
  {
    kind: "fill",
    from: "4.8",
    to: "4.9",
    rows: fillDeliveries,
  },
  {
    kind: "rebuild",
    from: "4.9",
    to: "4.10",
    table: "events",
    staging: "events_next",
    stagingSql: createEventsTableSql("events_next"),
    copy: {
      via: "sql",
      sql: "INSERT INTO events_next (seq, delivery_id, attempt_id, event, at, detail) SELECT e.seq, d.delivery_id, e.attempt_id, e.event, e.at, e.detail FROM events e JOIN deliveries d ON d.message_id = e.message_id ORDER BY e.seq",
    },
    after: [MESSAGE_EVENTS_VIEW_SQL],
  },
  {
    kind: "fill",
    from: "4.10",
    to: "4.11",
    rows: fillDeliveryEndpoints,
  },
  {
    kind: "rebuild",
    from: "4.11",
    to: "4.12",
    table: "deliveries",
    staging: "deliveries_next",
    stagingSql: `
DROP VIEW IF EXISTS message_events;
DROP TRIGGER IF EXISTS deliveries_role_differs;
DROP TRIGGER IF EXISTS deliveries_role_differs_on_assign;
DROP TRIGGER IF EXISTS deliveries_identity_immutable;
${createDeliveriesTableSql("deliveries_next")}`,
    copy: {
      via: "sql",
      sql: `INSERT INTO deliveries_next (
                delivery_id,
                message_id,
                endpoint_id,
                state,
                holder,
                attempt_id,
                attempt_count,
                lease_until,
                presented_at,
                confirmed_at
              )
              SELECT delivery_id,
                     message_id,
                     endpoint_id,
                     state,
                     holder,
                     attempt_id,
                     attempt_count,
                     lease_until,
                     presented_at,
                     confirmed_at
                FROM deliveries
               ORDER BY delivery_id`,
    },
    after: [
      DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
      DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL,
      DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL,
      MESSAGE_EVENTS_VIEW_SQL,
    ],
  },
  {
    kind: "rebuild",
    from: "4.12",
    to: "4.13",
    table: "messages",
    staging: MIGRATION_STAGING_TABLE,
    stagingSql: `
DROP TRIGGER IF EXISTS deliveries_role_differs;
DROP TRIGGER IF EXISTS deliveries_role_differs_on_assign;
${createMessagesTableSql(MIGRATION_STAGING_TABLE)}`,
    copy: {
      via: "rows",
      rows: copyStageFourMessages,
    },
    after: [
      MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL,
      DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
      DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL,
      DELIVERIES_ENDPOINT_STATE_INDEX_SQL,
    ],
  },
];

function rowCount(db: Database.Database, table: string): number {
  return (
    db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as {
      count: number;
    }
  ).count;
}

function rebuildStepTable(
  db: Database.Database,
  step: RebuildMigrationStep,
  options: MigrationOptions,
): void {
  db.exec(step.stagingSql);

  const sourceCount = rowCount(db, step.table);

  if (step.copy.via === "sql") {
    db.exec(step.copy.sql);
  } else {
    step.copy.rows(db, step.staging, options);
  }

  const copiedCount = rowCount(db, step.staging);

  if (copiedCount !== sourceCount) {
    throw new BridgeDatabaseError(
      `migration row-count mismatch: source=${sourceCount} copied=${copiedCount}`,
    );
  }

  db.exec(`DROP TABLE ${step.table};`);

  if (options.pauseAfterDestructiveDdl) {
    writeErrorRecord("agent-bridge migration paused after destructive DDL");
    const signal = new Int32Array(new SharedArrayBuffer(4));
    for (;;) {
      Atomics.wait(signal, 0, 0);
    }
  }

  db.exec(`ALTER TABLE ${step.staging} RENAME TO ${step.table};`);

  for (const statement of step.after) {
    db.exec(statement);
  }

  if (options.failAfterDestructiveDdl) {
    throw new BridgeDatabaseError(
      "injected migration failure after destructive DDL",
    );
  }
}

/*
 * One step, in the order the deployment guide documents: new table, every
 * row copied, the count checked, the old table dropped, the rename, the
 * indexes rebuilt, and only then the version. Every name in that order
 * comes off the step, so this function is the order and nothing more. The
 * caller runs it inside `BEGIN IMMEDIATE`, so a step that throws takes the
 * ones before it with it and the database is left on the version it
 * started on.
 */
function applyMigrationStep(
  db: Database.Database,
  step: MigrationStep,
  options: MigrationOptions,
): void {
  if (step.kind === "rebuild") {
    rebuildStepTable(db, step, options);
  } else if (step.kind === "ddl") {
    for (const statement of step.statements) {
      db.exec(statement);
    }
  } else {
    step.rows(db, options);
  }

  const updateVersion = db
    .prepare(
      `UPDATE meta
          SET v = ?
        WHERE k = 'schema_version'
          AND v = ?`,
    )
    .run(step.to, step.from);

  if (updateVersion.changes !== 1) {
    throw new BridgeDatabaseError("schema_version changed during migration");
  }
}

function migrationBackupStamp(now: Date): string {
  const iso = now.toISOString();
  return `${iso.slice(0, 10).replaceAll("-", "")}-${iso
    .slice(11, 19)
    .replaceAll(":", "")}`;
}

function sqliteStringLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function assertBackupIntegrity(backupPath: string): void {
  const backup = new Database(backupPath, {
    readonly: true,
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    backup.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);
    const integrity = String(
      backup.pragma("integrity_check", {
        simple: true,
      }),
    );
    if (integrity !== "ok") {
      throw new BridgeDatabaseError(
        `backup PRAGMA integrity_check failed: ${integrity}`,
      );
    }
  } finally {
    backup.close();
  }
}

function removeOwnedMigrationLock(db: Database.Database, value: string): void {
  const remove = db.transaction(() =>
    db
      .prepare(
        `DELETE FROM meta
          WHERE k = ?
            AND v = ?`,
      )
      .run(MIGRATION_LOCK_KEY, value),
  );
  const result = remove.immediate();

  if (result.changes !== 1) {
    throw new BridgeDatabaseError(
      "migration lock changed before it could be removed",
    );
  }
}

export interface CutoverPrecheckReport {
  passed: boolean;
  lines: string[];
}

export type CutoverPrecheck = (
  dbPath: string,
  mapping: EndpointMapping,
  configPaths: readonly string[],
) => CutoverPrecheckReport;

let cutoverPrecheck: CutoverPrecheck | null = null;

export function registerCutoverPrecheck(fn: CutoverPrecheck): void {
  cutoverPrecheck = fn;
}

function cutoverFrom(version: string): boolean {
  return version === "4.10" || version === "4.11" || version === "4.12";
}

function withBinaryCheck(
  lines: readonly string[],
  planFinal: string,
): string[] {
  return lines.map((line) => {
    if (!line.startsWith("precheck 1b:")) {
      return line;
    }

    return planFinal === SCHEMA_VERSION
      ? `precheck 1b: OK binary schema_version=${SCHEMA_VERSION}`
      : `precheck 1b: NG binary schema_version=${SCHEMA_VERSION} plan=${planFinal}`;
  });
}

function precheckPasses(lines: readonly string[]): boolean {
  return lines.every((line) => /^precheck [^:]+: (?:OK|対象外) /.test(line));
}

function assertCutoverPrecheck(
  dbPath: string,
  options: MigrationOptions,
  plan: readonly MigrationStep[],
): void {
  const planFinal = plan[plan.length - 1]?.to ?? "";

  if (options.mapping === undefined) {
    throw new BridgeDatabaseError(
      "migration refused by precheck:\nprecheck 3: NG --mapping is required",
    );
  }

  if (cutoverPrecheck === null) {
    throw new BridgeDatabaseError(
      "migration refused by precheck:\nprecheck 1b: NG cutover precheck is not registered",
    );
  }

  const report = cutoverPrecheck(
    dbPath,
    options.mapping,
    options.configPaths ?? [],
  );
  const lines = withBinaryCheck(report.lines, planFinal);

  if (!precheckPasses(lines)) {
    throw new BridgeDatabaseError(
      `migration refused by precheck:\n${lines.join("\n")}`,
    );
  }
}

/*
 * The ladder is a parameter rather than a `MigrationOptions` field
 * because `migrateFixedBridgeDatabase` forwards its options untouched: a
 * field there would put the ladder within reach of `bridge-init
 * --migrate`. Only a caller holding this function can replace it, and
 * only the tests do.
 */
export function migrateBridgeDatabaseAtPath(
  dbPath: string,
  options: MigrationOptions = {},
  steps: readonly MigrationStep[] = MIGRATION_STEPS,
): MigrationMetadata {
  if (options.mapping !== undefined) {
    validateEndpointMapping(options.mapping);
  }

  if (!existsSync(dbPath)) {
    throw new BridgeDatabaseError(
      `bridge database does not exist: ${dbPath}; initialize version ${LEGACY_SCHEMA_VERSION} before migrating`,
    );
  }

  const db = new Database(dbPath, {
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });
  let migrationLockValue: string | null = null;
  let migrationCommitted = false;
  let backupPath: string | null = null;

  try {
    db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);
    /*
     * Rebuilds drop tables that still have children. FK stays off for
     * this connection only; foreign_key_check runs before COMMIT.
     */
    db.pragma("foreign_keys = OFF");

    const activeLock = readMigrationLock(db);
    if (activeLock !== null) {
      throw new BridgeDatabaseError(
        `previous ${formatMigrationLock(
          activeLock,
        )}; restore from the backup before retrying`,
      );
    }

    const preflightGetMeta = db.prepare("SELECT v FROM meta WHERE k = ?");
    const preflightSchema = preflightGetMeta.get("schema_version") as
      { v: string } | undefined;
    const preflightRoot = preflightGetMeta.get("root_id") as
      { v: string } | undefined;

    if (!preflightSchema?.v) {
      throw new BridgeDatabaseError("meta.schema_version is missing");
    }
    if (!preflightRoot?.v) {
      throw new BridgeDatabaseError("meta.root_id is missing");
    }
    assertRootId(preflightRoot.v, "meta.root_id");

    const migrationTarget = options.stopAt ?? SCHEMA_VERSION;
    const preflightPlan = planMigration(
      preflightSchema.v,
      steps,
      migrationTarget,
    );
    if (preflightPlan.length === 0) {
      throw new BridgeDatabaseError(
        `schema_version is already ${migrationTarget}; there is nothing to migrate`,
      );
    }

    /*
     * The six cutover checks (design v12 D-5, v21 decision 5) are
     * evaluated on a 4.10 database: check 3 reads deliveries, and the
     * earlier tables do not have them. An origin below 4.10 therefore
     * walks to 4.10 first, in its own transaction with its own backup,
     * then runs the checks, then walks 4.10 -> 4.13. A failing check
     * leaves the database at 4.10, which the main binary still opens and
     * which the origin's backup restores; nothing irreversible has
     * happened. Without this split a 4.1 origin reached the destructive
     * steps with no check at all (Grok review of part B).
     */
    if (
      options.stopAt === undefined &&
      options.skipCutoverChecks !== true &&
      !cutoverFrom(preflightSchema.v) &&
      preflightPlan.some((step) => step.from === "4.10")
    ) {
      db.close();
      migrateBridgeDatabaseAtPath(
        dbPath,
        { ...options, stopAt: "4.10" },
        steps,
      );
      return migrateBridgeDatabaseAtPath(dbPath, options, steps);
    }

    const integrity = String(db.pragma("integrity_check", { simple: true }));
    if (integrity !== "ok") {
      throw new BridgeDatabaseError(
        `PRAGMA integrity_check failed before migration: ${integrity}`,
      );
    }

    backupPath = `${dbPath}.pre-${
      preflightSchema.v
    }-${migrationBackupStamp(new Date())}`;
    /*
     * SQLite refuses a non-empty target, but the wording depends on the
     * build ("file is not a database" here). Say what happened ourselves
     * and never overwrite a backup that is already there.
     */
    if (existsSync(backupPath)) {
      throw new BridgeDatabaseError(
        `backup path already exists; refusing to overwrite it: ${backupPath}`,
      );
    }
    db.exec(`VACUUM INTO ${sqliteStringLiteral(backupPath)}`);
    assertBackupIntegrity(backupPath);

    if (cutoverFrom(preflightSchema.v) && options.skipCutoverChecks !== true) {
      assertCutoverPrecheck(dbPath, options, preflightPlan);
    }

    const requestedLock = JSON.stringify({
      pid: process.pid,
      started_at: new Date().toISOString(),
    });
    const acquireLock = db.transaction(() =>
      db
        .prepare("INSERT OR IGNORE INTO meta (k, v) VALUES (?, ?)")
        .run(MIGRATION_LOCK_KEY, requestedLock),
    );
    const acquired = acquireLock.immediate();

    if (acquired.changes !== 1) {
      const racedLock = readMigrationLock(db);
      if (racedLock !== null) {
        throw new BridgeDatabaseError(
          `previous ${formatMigrationLock(
            racedLock,
          )}; restore from the backup before retrying`,
        );
      }
      throw new BridgeDatabaseError("migration lock could not be acquired");
    }
    migrationLockValue = requestedLock;

    const migrate = db.transaction((): BridgeMetadata => {
      const getMeta = db.prepare("SELECT v FROM meta WHERE k = ?");
      const schema = getMeta.get("schema_version") as { v: string } | undefined;
      const root = getMeta.get("root_id") as { v: string } | undefined;

      if (!schema?.v) {
        throw new BridgeDatabaseError("meta.schema_version is missing");
      }

      if (!root?.v) {
        throw new BridgeDatabaseError("meta.root_id is missing");
      }

      assertRootId(root.v, "meta.root_id");

      const planned = planMigration(schema.v, steps, migrationTarget);

      if (planned.length === 0) {
        throw new BridgeDatabaseError(
          `schema_version is already ${migrationTarget}; there is nothing to migrate`,
        );
      }

      for (const step of planned) {
        applyMigrationStep(db, step, options);
      }

      const violations = db.prepare("PRAGMA foreign_key_check").all();

      if (violations.length > 0) {
        throw new BridgeDatabaseError(
          `PRAGMA foreign_key_check failed: ${violations.length} row(s)`,
        );
      }

      return {
        dbPath,
        rootId: root.v,
        schemaVersion: migrationTarget,
      };
    });

    const metadata = migrate.immediate();
    migrationCommitted = true;

    if (migrationLockValue === null || backupPath === null) {
      throw new BridgeDatabaseError(
        "migration completed without its lock or backup identity",
      );
    }

    removeOwnedMigrationLock(db, migrationLockValue);
    migrationLockValue = null;

    return {
      ...metadata,
      backupPath,
    };
  } catch (error) {
    if (
      migrationLockValue !== null &&
      !migrationCommitted &&
      !db.inTransaction
    ) {
      try {
        removeOwnedMigrationLock(db, migrationLockValue);
        migrationLockValue = null;
      } catch (cleanupError) {
        const cleanupDetail =
          cleanupError instanceof Error
            ? cleanupError.message
            : String(cleanupError);
        throw new BridgeDatabaseError(
          `bridge migration failed and lock cleanup failed: ${cleanupDetail}; restore from backup ${backupPath ?? "(unknown)"}`,
        );
      }
    }

    if (migrationCommitted) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new BridgeDatabaseError(
        `bridge migration committed but lock cleanup failed: ${detail}; restore from backup ${backupPath ?? "(unknown)"}`,
      );
    }

    if (error instanceof BridgeDatabaseError) {
      throw error;
    }

    const detail = error instanceof Error ? error.message : String(error);
    throw new BridgeDatabaseError(`bridge migration failed: ${detail}`);
  } finally {
    if (db.open) {
      db.close();
    }
  }
}

export function migrateFixedBridgeDatabase(
  options: MigrationOptions = {},
): MigrationMetadata {
  return migrateBridgeDatabaseAtPath(getBridgeDbPath(), options);
}

function readDatabaseIdentity(dbPath: string): {
  schemaVersion: string;
  rootId: string;
} {
  const db = new Database(dbPath, {
    readonly: true,
    fileMustExist: true,
    timeout: BUSY_TIMEOUT_MS,
  });

  try {
    const read = db.prepare("SELECT v FROM meta WHERE k = ?");
    const schema = read.get("schema_version") as { v: string } | undefined;
    const root = read.get("root_id") as { v: string } | undefined;

    if (!schema?.v || !root?.v) {
      throw new BridgeDatabaseError(
        "rehearsal source is missing schema_version or root_id",
      );
    }

    return {
      schemaVersion: schema.v,
      rootId: root.v,
    };
  } finally {
    db.close();
  }
}

function newestCompatibleBackup(
  dbPath: string,
  identity: { schemaVersion: string; rootId: string },
): string | null {
  const directory = dirname(dbPath);
  const prefix = `${basename(dbPath)}.pre-`;
  const names = readdirSync(directory)
    .filter((name) => name.startsWith(prefix))
    .sort()
    .reverse();

  for (const name of names) {
    const path = join(directory, name);

    try {
      const backup = readDatabaseIdentity(path);

      if (
        backup.schemaVersion === identity.schemaVersion &&
        backup.rootId === identity.rootId
      ) {
        return path;
      }
    } catch {
      continue;
    }
  }

  return null;
}

function copyLiveDatabase(dbPath: string, snapshot: string): void {
  copyFileSync(dbPath, snapshot);
  const wal = `${dbPath}-wal`;

  if (existsSync(wal)) {
    copyFileSync(wal, `${snapshot}-wal`);
  }
}

function removeSnapshotFiles(snapshot: string): void {
  const directory = dirname(snapshot);
  const base = basename(snapshot);

  if (!existsSync(directory)) {
    return;
  }

  for (const name of readdirSync(directory)) {
    if (
      name === base ||
      name.startsWith(`${base}.`) ||
      name.startsWith(`${base}-`)
    ) {
      unlinkSync(join(directory, name));
    }
  }
}

function measureRehearsal(db: Database.Database): string[] {
  const now = Date.now();
  const sentAt = new Date(now).toISOString();
  const presentedNow = sentAt;
  const presentedOld = new Date(now - 60 * 60 * 1000).toISOString();
  const cutoff = new Date(now - PRESENTED_TTL_MS).toISOString();

  const addEndpoint = db.prepare(
    `INSERT INTO endpoints (
       endpoint_id, role, name, created_at, retired_at
     ) VALUES (?, ?, ?, ?, NULL)`,
  );
  const endpoint = (role: Role, name: string): string => {
    const id = randomUUID();
    addEndpoint.run(id, role, name, sentAt);
    return id;
  };
  const sourceId = endpoint("claude", "rehearse-src");
  const hereId = endpoint("codex", "rehearse-here");
  const thereId = endpoint("codex", "rehearse-there");
  const n2a = endpoint("codex", "rehearse-n2-a");
  const n2b = endpoint("codex", "rehearse-n2-b");
  const insertMessage = db.prepare(
    `INSERT INTO messages (
       message_id, from_role, source_endpoint_id,
       legacy_to_tag, legacy_from_tag,
       subject, body, envelope_sha256, envelope_version,
       body_sha256, attempt_count, sent_at
     ) VALUES (
       ?, 'claude', ?, NULL, NULL, ?, 'body', 'aa', 2, 'bb', 0, ?
     )`,
  );
  const insertPending = db.prepare(
    `INSERT INTO deliveries (
       message_id, endpoint_id, state, holder, attempt_id,
       attempt_count, lease_until, presented_at, confirmed_at
     ) VALUES (?, ?, ?, NULL, NULL, 0, NULL, NULL, NULL)`,
  );
  const insertLeased = db.prepare(
    `INSERT INTO deliveries (
       message_id, endpoint_id, state, holder, attempt_id,
       attempt_count, lease_until, presented_at, confirmed_at
     ) VALUES (
       ?, ?, 'leased', 'rehearse', ?, 0, ?, NULL, NULL
     )`,
  );
  const insertPresented = db.prepare(
    `INSERT INTO deliveries (
       message_id, endpoint_id, state, holder, attempt_id,
       attempt_count, lease_until, presented_at, confirmed_at
     ) VALUES (
       ?, ?, 'presented', 'rehearse', ?, 0, NULL, ?, NULL
     )`,
  );

  const n2Message = randomUUID();
  insertMessage.run(n2Message, sourceId, "n2", sentAt);
  const n2aDelivery = Number(
    insertPending.run(n2Message, n2a, "pending").lastInsertRowid,
  );
  insertPending.run(n2Message, n2b, "pending");
  const attempt = randomUUID();
  const leaseUntil = now + 60 * 60 * 1000;
  db.prepare(
    `UPDATE deliveries
        SET state = 'leased',
            holder = 'rehearse',
            attempt_id = ?,
            lease_until = ?,
            presented_at = NULL,
            confirmed_at = NULL
      WHERE delivery_id = ?`,
  ).run(attempt, leaseUntil, n2aDelivery);
  db.prepare(
    `UPDATE deliveries
        SET state = 'presented',
            holder = 'rehearse',
            attempt_id = ?,
            lease_until = NULL,
            presented_at = ?,
            confirmed_at = NULL
      WHERE delivery_id = ?`,
  ).run(attempt, presentedNow, n2aDelivery);
  db.prepare(
    `UPDATE deliveries
        SET state = 'confirmed',
            holder = 'rehearse',
            attempt_id = ?,
            lease_until = NULL,
            presented_at = ?,
            confirmed_at = ?
      WHERE delivery_id = ?`,
  ).run(attempt, presentedNow, presentedNow, n2aDelivery);
  const n2States = db
    .prepare(
      `SELECT endpoint_id, state
         FROM deliveries
        WHERE message_id = ?`,
    )
    .all(n2Message) as Array<{
    endpoint_id: string;
    state: string;
  }>;
  const stateA =
    n2States.find((row) => row.endpoint_id === n2a)?.state ?? "missing";
  const stateB =
    n2States.find((row) => row.endpoint_id === n2b)?.state ?? "missing";
  db.prepare("DELETE FROM deliveries WHERE message_id = ?").run(n2Message);
  db.prepare("DELETE FROM messages WHERE message_id = ?").run(n2Message);

  const add = (
    subject: string,
    endpoint: string,
    kind: "pending" | "leased" | "presented" | "bounced",
    when?: number | string,
  ): void => {
    const messageId = randomUUID();
    insertMessage.run(messageId, sourceId, subject, sentAt);

    if (kind === "pending" || kind === "bounced") {
      insertPending.run(
        messageId,
        endpoint,
        kind === "bounced" ? "bounced" : "pending",
      );
      return;
    }

    if (kind === "leased") {
      insertLeased.run(messageId, endpoint, randomUUID(), when);
      return;
    }

    insertPresented.run(messageId, endpoint, randomUUID(), when);
  };

  add("untagged", hereId, "pending");
  add("tagged-expiring", hereId, "pending");
  add("tagged-open", hereId, "pending");
  add("live-leased", hereId, "leased", now + 60 * 60 * 1000);
  add("expired-leased", hereId, "leased", now - 60 * 60 * 1000);
  add("bounced", hereId, "bounced");
  add("live-presented", hereId, "presented", presentedNow);
  add("expired-presented", hereId, "presented", presentedOld);
  add("elsewhere", thereId, "pending");

  const count = (sql: string, ...params: Array<string | number>): number =>
    (
      db.prepare(sql).get(...params) as {
        count: number;
      }
    ).count;
  const pendingHere = count(
    `SELECT COUNT(*) AS count
       FROM deliveries
      WHERE endpoint_id = ?
        AND state = 'pending'`,
    hereId,
  );
  const pendingElsewhere = count(
    `SELECT COUNT(*) AS count
       FROM deliveries d
       JOIN endpoints ep
         ON ep.endpoint_id = d.endpoint_id
      WHERE ep.role = 'codex'
        AND d.endpoint_id <> ?
        AND d.state = 'pending'`,
    hereId,
  );
  const expiredLeased = count(
    `SELECT COUNT(*) AS count
       FROM deliveries
      WHERE endpoint_id = ?
        AND state = 'leased'
        AND lease_until < ?`,
    hereId,
    now,
  );
  const expiredPresented = count(
    `SELECT COUNT(*) AS count
       FROM deliveries
      WHERE endpoint_id = ?
        AND state = 'presented'
        AND presented_at < ?`,
    hereId,
    cutoff,
  );

  return [
    `rehearse n2: A=${stateA} B=${stateB}`,
    `rehearse pending_here=${pendingHere}`,
    `rehearse pending_elsewhere=${pendingElsewhere}`,
    `rehearse expired_leased=${expiredLeased}`,
    `rehearse expired_presented=${expiredPresented}`,
  ];
}

/*
 * The live database is never opened for writing. A matching backup is a
 * file copy; otherwise the live file and its wal are copied. VACUUM INTO
 * on a WAL database can checkpoint the source.
 */
export function rehearseBridgeDatabaseAtPath(
  dbPath: string,
  options: MigrationOptions = {},
): string[] {
  const snapshot = `${dbPath}.rehearse-${randomUUID()}`;

  try {
    const identity = readDatabaseIdentity(dbPath);
    const backup = newestCompatibleBackup(dbPath, identity);

    if (backup === null) {
      copyLiveDatabase(dbPath, snapshot);
    } else {
      copyFileSync(backup, snapshot);
    }

    migrateBridgeDatabaseAtPath(snapshot, options);
    const db = new Database(snapshot, {
      fileMustExist: true,
      timeout: BUSY_TIMEOUT_MS,
    });

    try {
      db.pragma(`busy_timeout = ${BUSY_TIMEOUT_MS}`);
      return measureRehearsal(db);
    } finally {
      db.close();
    }
  } finally {
    /*
     * Nothing here opens the live file for writing, and a read-only open
     * does not move its mtime (measured: identical mtimeMs before and
     * after a select and an integrity_check), so there is nothing to put
     * back. Restoring the time with utimes would truncate it to whole
     * milliseconds and make b-17's exact comparison fail.
     */
    removeSnapshotFiles(snapshot);
  }
}

export function readServerForeignKeys(dbPath: string): number {
  const opened = openVerifiedDatabase(dbPath, true);

  try {
    return Number(
      opened.db.pragma("foreign_keys", {
        simple: true,
      }),
    );
  } finally {
    opened.db.close();
  }
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

    const migrationLock = readMigrationLock(db);
    if (migrationLock !== null) {
      throw new BridgeDatabaseError(
        `bridge database ${formatMigrationLock(migrationLock)}`,
      );
    }

    const integrity = String(db.pragma("integrity_check", { simple: true }));
    if (integrity !== "ok") {
      throw new BridgeDatabaseError(
        `PRAGMA integrity_check failed: ${integrity}`,
      );
    }

    const getMeta = db.prepare("SELECT v FROM meta WHERE k = ?");
    const root = getMeta.get("root_id") as { v: string } | undefined;
    const schema = getMeta.get("schema_version") as { v: string } | undefined;

    if (!root?.v) {
      throw new BridgeDatabaseError("meta.root_id is missing");
    }

    assertRootId(root.v, "meta.root_id");

    if (!schema?.v) {
      throw new BridgeDatabaseError("meta.schema_version is missing");
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

    const detail = error instanceof Error ? error.message : String(error);
    throw new BridgeDatabaseError(
      `bridge database verification failed: ${detail}`,
    );
  }
}

function toIso(epochMs: number): string {
  return new Date(epochMs).toISOString();
}

function requireRole(role: unknown): Role {
  if (role !== "claude" && role !== "codex") {
    throw new BridgeError("role must be claude or codex");
  }

  return role;
}

function requireCursor(value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new BridgeError(
      "cursor must be a positive integer taken from next_cursor",
    );
  }

  return value;
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
  if (typeof consumer !== "string" || consumer.length === 0) {
    throw new BridgeError("consumer must be a non-empty string");
  }

  return consumer;
}

interface ClaimedDeliveryRow {
  deliveryId: number;
  messageId: string;
  attemptId: string;
  subject: string;
  body: string;
  fromRole: Role;
  fromEndpoint: string | null;
  attemptCount: number;
  sentAt: string;
  sourceEndpointId: string;
  messageRowId: number;
  envelopeSha256: string;
  envelopeVersion: number;
  bodySha256: string;
  senderThreadId: string | null;
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
    const opened = openVerifiedDatabase(dbPath, false);
    return new BridgeBus(dbPath, opened.db, opened.metadata);
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.db.close();
  }

  setRolePolicy(key: RolePolicyKey, value: string): void {
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

  policyRoles(key: RolePolicyKey): Set<Role> {
    return this.readPolicyRoles(key);
  }

  /*
   * The only writer of this table. A server that meets a name it does not
   * know rejects the startup instead of adding the row, so a name reaches
   * the registry through an operator running `bridge-init
   * --add-endpoint` and no other way.
   */
  addEndpoint(role: Role, name: string, now = new Date()): EndpointRow {
    const endpointName = typeof name === "string" ? name : "";

    const nameProblem = endpointNameProblem(endpointName);
    if (nameProblem !== null) {
      throw new BridgeError(nameProblem);
    }

    const row: EndpointRow = {
      endpoint_id: randomUUID(),
      role: requireRole(role),
      name: endpointName,
      created_at: now.toISOString(),
      retired_at: null,
    };

    const add = this.db.transaction(() => {
      const existing = this.db
        .prepare(
          `SELECT endpoint_id
             FROM endpoints
            WHERE role = ?
              AND name = ?`,
        )
        .get(row.role, row.name) as { endpoint_id: string } | undefined;

      if (existing) {
        throw new BridgeError(
          `endpoint ${row.role}/${row.name} is already registered as ${existing.endpoint_id}`,
        );
      }

      this.db
        .prepare(
          `INSERT INTO endpoints (
             endpoint_id,
             role,
             name,
             created_at,
             retired_at
           ) VALUES (?, ?, ?, ?, NULL)`,
        )
        .run(row.endpoint_id, row.role, row.name, row.created_at);
    });

    add.immediate();
    return row;
  }

  /*
   * Three refusals, not one. A name that was never registered, a name
   * held by the other role and a name that has been retired are three
   * different mistakes in an operator's config, and one message for all
   * three sends whoever reads it looking in the wrong place.
   */
  resolveEndpoint(role: Role, name: string, allowRetired = false): EndpointRow {
    const rows = this.db
      .prepare(
        `SELECT endpoint_id,
                role,
                name,
                created_at,
                retired_at
           FROM endpoints
          WHERE name = ?`,
      )
      .all(name) as EndpointRow[];

    const mine = rows.find((row) => row.role === role);

    if (!mine) {
      throw new BridgeError(
        rows.length === 0
          ? `no endpoint named ${quoteForOneLine(name)} is registered; add it with bridge-init --add-endpoint`
          : `endpoint ${quoteForOneLine(name)} is registered for ${rows
              .map((row) => row.role)
              .sort()
              .join(",")}, not ${role}`,
      );
    }

    if (mine.retired_at !== null && !allowRetired) {
      throw new BridgeError(
        `endpoint ${role}/${name} was retired at ${mine.retired_at}`,
      );
    }

    return mine;
  }

  retireEndpoint(role: Role, name: string, now = new Date()): EndpointRow {
    /*
     * One immediate transaction: a send that lands between the pending
     * count and the UPDATE would leave a pending delivery on a retired
     * endpoint, whose server can no longer start to take it.
     */
    const run = this.db.transaction((): EndpointRow => {
      const endpoint = this.resolveEndpoint(role, name);
      const pending = this.db
        .prepare(
          `SELECT COUNT(*) AS count
           FROM deliveries
          WHERE endpoint_id = ?
            AND state = 'pending'`,
        )
        .get(endpoint.endpoint_id) as {
        count: number;
      };

      if (pending.count > 0) {
        throw new BridgeError(
          `endpoint ${role}/${name} has ${pending.count} pending delivery; refusing retirement without a transfer`,
        );
      }

      const retiredAt = now.toISOString();
      const updated = this.db
        .prepare(
          `UPDATE endpoints
            SET retired_at = ?
          WHERE endpoint_id = ?
            AND retired_at IS NULL`,
        )
        .run(retiredAt, endpoint.endpoint_id);

      if (updated.changes !== 1) {
        throw new BridgeError(`endpoint ${role}/${name} could not be retired`);
      }

      return {
        ...endpoint,
        retired_at: retiredAt,
      };
    });
    return run.immediate();
  }

  private readPolicyRoles(key: RolePolicyKey): Set<Role> {
    const row = this.db.prepare("SELECT v FROM meta WHERE k = ?").get(key) as
      { v: unknown } | undefined;

    return parseRolePolicy(key, row?.v);
  }

  private removedSendArguments(input: {
    toTag?: unknown;
    toEndpoint?: unknown;
    broadcast?: unknown;
    onTimeout?: unknown;
  }): void {
    const removed: string[] = [];
    if (input.toTag !== undefined && input.toTag !== null) {
      removed.push("to_tag");
    }
    if (input.broadcast !== undefined && input.broadcast !== null) {
      removed.push("broadcast");
    }
    if (input.onTimeout !== undefined && input.onTimeout !== null) {
      removed.push("on_timeout");
    }
    if (input.toEndpoint !== undefined && input.toEndpoint !== null) {
      removed.push("to_endpoint");
    }
    if (removed.length > 0) {
      throw new BridgeError(`refusing removed argument: ${removed.join(", ")}`);
    }
  }

  private endpointNames(value: unknown): string[] {
    if (!Array.isArray(value) || value.length === 0) {
      throw new BridgeError("to_endpoints must name at least one endpoint");
    }
    const names: string[] = [];
    const seen = new Set<string>();
    for (const item of value) {
      if (typeof item !== "string" || item.length === 0) {
        throw new BridgeError(
          "to_endpoints must be an array of endpoint names",
        );
      }
      if (seen.has(item)) {
        throw new BridgeError(`to_endpoints repeats ${item}`);
      }
      seen.add(item);
      names.push(item);
    }
    return names;
  }

  private deliverSend(input: {
    fromRole: Role;
    toRole: Role;
    subject: unknown;
    body: unknown;
    messageId?: unknown;
    senderThreadId?: unknown;
    toTag?: unknown;
    toEndpoint?: unknown;
    broadcast?: unknown;
    fromTag?: unknown;
    sourceEndpoint?: EndpointRow | null;
    onTimeout?: unknown;
    toEndpoints?: unknown;
    now?: number;
  }): StoredSendResult {
    const fromRole = requireRole(input.fromRole);
    const toRole = requireRole(input.toRole);
    if (fromRole === toRole) {
      throw new BridgeError("from_role and to_role must differ");
    }
    this.removedSendArguments(input);
    const sourceEndpoint = input.sourceEndpoint ?? null;
    if (sourceEndpoint === null) {
      throw new BridgeError("source endpoint is required");
    }
    if (sourceEndpoint.role !== fromRole) {
      throw new BridgeError("source endpoint role does not match from_role");
    }
    const names = this.endpointNames(input.toEndpoints);
    const subject = normalizeSubject(input.subject);
    const body = validateBody(input.body);
    const messageId =
      input.messageId === undefined
        ? randomUUID()
        : validateMessageId(input.messageId);
    let senderThreadId: string | null = null;
    if (input.senderThreadId !== undefined && input.senderThreadId !== null) {
      if (typeof input.senderThreadId !== "string") {
        throw new BridgeError("thread_id must be a string when provided");
      }
      senderThreadId = input.senderThreadId;
    }
    const envelopeHash = envelopeHashSeam.compute(fromRole, subject, body);
    const bodyHash = sha256(body);
    const now = input.now ?? Date.now();
    const sentAt = toIso(now);
    const destinationRole = oppositeRole(fromRole);
    type SendOutcome =
      | { kind: "stored"; existed: boolean; added: string[] }
      | { kind: "conflict"; senderMismatch: boolean };
    const operation = this.db.transaction((): SendOutcome => {
      const retainedDelivery = this.db.prepare(
        `SELECT delivery_id
             FROM deliveries
            WHERE message_id = ?
              AND endpoint_id = ?`,
      );
      const destinations = names.map((name) => {
        const endpoint = this.resolveEndpoint(destinationRole, name, true);
        if (endpoint.retired_at !== null) {
          const retained = retainedDelivery.get(
            messageId,
            endpoint.endpoint_id,
          );
          if (retained === undefined) {
            throw new BridgeError(
              `endpoint ${destinationRole}/${name} was retired at ${endpoint.retired_at}`,
            );
          }
        }
        return endpoint;
      });
      const existing = this.db
        .prepare(
          `SELECT from_role,
                    source_endpoint_id,
                    envelope_sha256
               FROM messages
              WHERE message_id = ?`,
        )
        .get(messageId) as
        | {
            from_role: Role;
            source_endpoint_id: string;
            envelope_sha256: string;
          }
        | undefined;
      if (existing) {
        const first = this.db
          .prepare(
            `SELECT delivery_id
                 FROM deliveries
                WHERE message_id = ?
                ORDER BY delivery_id
                LIMIT 1`,
          )
          .get(messageId) as { delivery_id: number } | undefined;
        if (!first) {
          throw new BridgeDatabaseError(
            `delivery not found for existing message ${messageId}`,
          );
        }
        const senderMismatch =
          existing.from_role !== fromRole ||
          existing.source_endpoint_id !== sourceEndpoint.endpoint_id;
        if (senderMismatch || existing.envelope_sha256 !== envelopeHash) {
          this.insertEvent(
            first.delivery_id,
            null,
            "send_conflict",
            sentAt,
            JSON.stringify(
              senderMismatch
                ? { sender_mismatch: true }
                : {
                    existing_envelope_sha256: existing.envelope_sha256,
                    attempted_envelope_sha256: envelopeHash,
                  },
            ),
          );
          return {
            kind: "conflict",
            senderMismatch,
          };
        }
      } else {
        this.db
          .prepare(
            `INSERT INTO messages (
                 message_id, from_role, source_endpoint_id,
                 subject, body, envelope_sha256, envelope_version,
                 body_sha256, sender_thread_id, sent_at
               ) VALUES (?, ?, ?, ?, ?, ?, 2, ?, ?, ?)`,
          )
          .run(
            messageId,
            fromRole,
            sourceEndpoint.endpoint_id,
            subject,
            body,
            envelopeHash,
            bodyHash,
            senderThreadId,
            sentAt,
          );
      }
      const findDelivery = this.db.prepare(
        `SELECT delivery_id
             FROM deliveries
            WHERE message_id = ?
              AND endpoint_id = ?`,
      );
      const insertDelivery = this.db.prepare(
        `INSERT INTO deliveries (
             message_id, endpoint_id, state
           ) VALUES (?, ?, 'pending')`,
      );
      const added: string[] = [];
      for (const destination of destinations) {
        const already = findDelivery.get(messageId, destination.endpoint_id) as
          { delivery_id: number } | undefined;
        if (already) {
          continue;
        }
        const inserted = insertDelivery.run(messageId, destination.endpoint_id);
        this.insertEvent(
          Number(inserted.lastInsertRowid),
          null,
          "sent",
          sentAt,
          null,
        );
        added.push(destination.name);
      }
      return {
        kind: "stored",
        existed: existing !== undefined,
        added,
      };
    });
    const outcome = operation.immediate();
    if (outcome.kind === "conflict") {
      throw new BridgeConflictError(
        outcome.senderMismatch
          ? `message_id ${messageId} belongs to a different sender`
          : `message_id ${messageId} already exists with a different envelope`,
      );
    }
    return {
      messageId,
      subject,
      idempotent: outcome.existed,
      toTag: null,
      destinationRequiresTag: null,
      added: outcome.added,
    };
  }

  private recoverDeliveries(
    role: Role,
    now: number,
    endpointId: string | null,
  ): RecoveryResult {
    const nowIso = toIso(now);
    const presentedCutoff = toIso(now - PRESENTED_TTL_MS);
    const expired = this.db
      .prepare(
        `SELECT d.delivery_id AS deliveryId,
                d.message_id AS messageId,
                d.attempt_id AS attemptId
           FROM deliveries d
           JOIN endpoints ep
             ON ep.endpoint_id = d.endpoint_id
          WHERE ep.role = ?
            AND d.state = 'leased'
            AND d.lease_until < ?
            AND (? IS NULL OR d.endpoint_id = ?)
          ORDER BY d.delivery_id`,
      )
      .all(role, now, endpointId, endpointId) as Array<{
      deliveryId: number;
      messageId: string;
      attemptId: string | null;
    }>;
    const releaseLease = this.db.prepare(
      `UPDATE deliveries
          SET state = 'pending',
              holder = NULL,
              attempt_id = NULL,
              lease_until = NULL
        WHERE delivery_id = ?
          AND state = 'leased'
          AND lease_until < ?`,
    );
    for (const row of expired) {
      const update = releaseLease.run(row.deliveryId, now);
      this.assertOneChange(
        update.changes,
        `leased->pending recovery failed for ${row.messageId}`,
      );
      this.insertEvent(
        row.deliveryId,
        row.attemptId,
        "lease_expired",
        nowIso,
        JSON.stringify({ recovered_by_role: role }),
      );
    }
    const stale = this.db
      .prepare(
        `SELECT d.delivery_id AS deliveryId,
                d.message_id AS messageId,
                d.attempt_id AS attemptId
           FROM deliveries d
           JOIN endpoints ep
             ON ep.endpoint_id = d.endpoint_id
          WHERE ep.role = ?
            AND d.state = 'presented'
            AND d.confirmed_at IS NULL
            AND d.presented_at < ?
            AND (? IS NULL OR d.endpoint_id = ?)
          ORDER BY d.delivery_id`,
      )
      .all(role, presentedCutoff, endpointId, endpointId) as Array<{
      deliveryId: number;
      messageId: string;
      attemptId: string | null;
    }>;
    const releasePresented = this.db.prepare(
      `UPDATE deliveries
          SET state = 'pending',
              holder = NULL,
              attempt_id = NULL,
              lease_until = NULL,
              presented_at = NULL
        WHERE delivery_id = ?
          AND state = 'presented'
          AND confirmed_at IS NULL
          AND presented_at < ?`,
    );
    for (const row of stale) {
      const update = releasePresented.run(row.deliveryId, presentedCutoff);
      this.assertOneChange(
        update.changes,
        `presented->pending recovery failed for ${row.messageId}`,
      );
      this.insertEvent(
        row.deliveryId,
        row.attemptId,
        "requeued",
        nowIso,
        JSON.stringify({ recovered_by_role: role }),
      );
    }
    return {
      leaseExpired: expired.length,
      requeued: stale.length,
      bounced: 0,
      fallbackDemoted: 0,
    };
  }

  private claimDeliveries(
    endpoint: EndpointRow,
    consumer: string,
    limit: number,
    now: number,
    messageId: string | null,
  ): ClaimedDeliveryRow[] {
    const rows = this.db
      .prepare(
        `SELECT d.delivery_id AS deliveryId,
                d.attempt_count AS attemptCount,
                d.message_id AS messageId,
                m.id AS messageRowId,
                m.from_role AS fromRole,
                m.subject AS subject,
                m.body AS body,
                m.body_sha256 AS bodySha256,
                m.envelope_sha256 AS envelopeSha256,
                m.envelope_version AS envelopeVersion,
                m.sent_at AS sentAt,
                m.source_endpoint_id AS sourceEndpointId,
                m.sender_thread_id AS senderThreadId,
                src.name AS fromEndpoint
           FROM deliveries d
           JOIN messages m
             ON m.message_id = d.message_id
           LEFT JOIN endpoints src
             ON src.endpoint_id = m.source_endpoint_id
          WHERE d.endpoint_id = ?
            AND d.state = 'pending'
            AND (? IS NULL OR d.message_id = ?)
          ORDER BY d.delivery_id
          LIMIT ?`,
      )
      .all(endpoint.endpoint_id, messageId, messageId, limit) as Array<
      Omit<ClaimedDeliveryRow, "attemptId">
    >;
    const lease = this.db.prepare(
      `UPDATE deliveries
          SET state = 'leased',
              holder = ?,
              attempt_id = ?,
              attempt_count = attempt_count + 1,
              lease_until = ?,
              presented_at = NULL
        WHERE delivery_id = ?
          AND endpoint_id = ?
          AND state = 'pending'`,
    );
    const reject = this.db.prepare(
      `UPDATE deliveries
          SET state = 'rejected',
              lease_until = NULL
        WHERE delivery_id = ?
          AND state = 'leased'
          AND attempt_id = ?
          AND holder = ?`,
    );
    const claimedAt = toIso(now);
    const claimed: ClaimedDeliveryRow[] = [];
    for (const row of rows) {
      const attemptId = randomUUID();
      const update = lease.run(
        consumer,
        attemptId,
        now + CLAIM_LEASE_MS,
        row.deliveryId,
        endpoint.endpoint_id,
      );
      this.assertOneChange(
        update.changes,
        `pending->leased failed for ${row.messageId}`,
      );
      this.insertEvent(
        row.deliveryId,
        attemptId,
        "claimed",
        claimedAt,
        JSON.stringify({ consumer }),
      );
      if (sha256(row.body) !== row.bodySha256) {
        const rejected = reject.run(row.deliveryId, attemptId, consumer);
        this.assertOneChange(
          rejected.changes,
          `leased->rejected failed for ${row.messageId}`,
        );
        this.insertEvent(
          row.deliveryId,
          attemptId,
          "rejected",
          claimedAt,
          "body_sha256 mismatch",
        );
        continue;
      }
      claimed.push({
        ...row,
        attemptId,
        attemptCount: row.attemptCount + 1,
      });
    }
    return claimed;
  }

  private asClaimed(
    row: ClaimedDeliveryRow,
    consumer: string,
    now: number,
  ): ClaimedMessage {
    return {
      id: row.messageRowId,
      message_id: row.messageId,
      from_role: row.fromRole,
      subject: row.subject,
      body: row.body,
      envelope_sha256: row.envelopeSha256,
      envelope_version: row.envelopeVersion,
      body_sha256: row.bodySha256,
      sender_thread_id: row.senderThreadId,
      source_endpoint_id: row.sourceEndpointId,
      status: "claimed",
      attempt_id: row.attemptId,
      consumer,
      lease_expires_at: now + CLAIM_LEASE_MS,
      attempt_count: row.attemptCount,
      sent_at: row.sentAt,
      presented_at: null,
      acked_at: null,
      redelivery: row.attemptCount > 1,
    } as unknown as ClaimedMessage;
  }

  private presentDeliveries(
    endpoint: EndpointRow,
    consumerInput: string,
    messages: ReadonlyArray<{
      messageId: string;
      attemptId: string;
    }>,
    now: number,
  ): void {
    const consumer = requireConsumer(consumerInput);
    const presentedAt = toIso(now);
    const update = this.db.prepare(
      `UPDATE deliveries
          SET state = 'presented',
              presented_at = ?,
              lease_until = NULL
        WHERE message_id = ?
          AND endpoint_id = ?
          AND state = 'leased'
          AND attempt_id = ?
          AND holder = ?
        RETURNING delivery_id`,
    );
    for (const message of messages) {
      const rows = update.all(
        presentedAt,
        validateMessageId(message.messageId),
        endpoint.endpoint_id,
        validateAttemptId(message.attemptId),
        consumer,
      ) as Array<{ delivery_id: number }>;
      this.assertOneChange(
        rows.length,
        `leased->presented failed for ${message.messageId}`,
      );
      this.insertEvent(
        rows[0].delivery_id,
        message.attemptId,
        "presented",
        presentedAt,
        JSON.stringify({ consumer }),
      );
    }
  }

  private mapDeliveryStatus(state: string): LatestMessageState["status"] {
    switch (state) {
      case "pending":
        return "stored";
      case "leased":
        return "claimed";
      case "confirmed":
        return "acked";
      case "presented":
      case "rejected":
      case "bounced":
        return state;
      case "cancelled":
        return "cancelled";
      default:
        return "rejected";
    }
  }

  private deliveryLatest(
    messageId: string,
    endpoint: EndpointRow,
  ): LatestMessageState | null {
    const row = this.db
      .prepare(
        `SELECT state, attempt_id, attempt_count,
                presented_at, confirmed_at
           FROM deliveries
          WHERE message_id = ?
            AND endpoint_id = ?`,
      )
      .get(messageId, endpoint.endpoint_id) as
      | {
          state: string;
          attempt_id: string | null;
          attempt_count: number;
          presented_at: string | null;
          confirmed_at: string | null;
        }
      | undefined;
    if (!row) {
      return null;
    }
    return {
      message_id: messageId,
      status: this.mapDeliveryStatus(row.state),
      attempt_id: row.attempt_id,
      attempt_count: row.attempt_count,
      presented_at: row.presented_at,
      acked_at: row.confirmed_at,
    };
  }

  private confirmDelivery(
    endpoint: EndpointRow,
    messageIdInput: unknown,
    attemptIdInput: unknown,
    now: number,
    consumerInput: unknown,
  ): LatestMessageState {
    const consumer = requireConsumer(consumerInput);
    const messageId = validateMessageId(messageIdInput);
    const attemptId = validateAttemptId(attemptIdInput);
    const ackedAt = toIso(now);
    const operation = this.db.transaction(() => {
      const updated = this.db
        .prepare(
          `UPDATE deliveries
              SET state = 'confirmed',
                  confirmed_at = ?
            WHERE message_id = ?
              AND endpoint_id = ?
              AND state = 'presented'
              AND attempt_id = ?
              AND holder = ?
            RETURNING delivery_id, attempt_count, presented_at`,
        )
        .all(
          ackedAt,
          messageId,
          endpoint.endpoint_id,
          attemptId,
          consumer,
        ) as Array<{
        delivery_id: number;
        attempt_count: number;
        presented_at: string | null;
      }>;
      if (updated.length !== 1) {
        return {
          ok: false as const,
          latest: this.deliveryLatest(messageId, endpoint),
        };
      }
      this.insertEvent(
        updated[0].delivery_id,
        attemptId,
        "acked",
        ackedAt,
        null,
      );
      return {
        ok: true as const,
        state: {
          message_id: messageId,
          status: "acked" as const,
          attempt_id: attemptId,
          attempt_count: updated[0].attempt_count,
          presented_at: updated[0].presented_at,
          acked_at: ackedAt,
        },
      };
    });
    const result = operation.immediate();
    if (!result.ok) {
      throw new BridgeTransitionError(
        `bridge_ack rejected for ${messageId}: this process is not the holder of the delivery at endpoint ${endpoint.name} under attempt ${attemptId}`,
        result.latest,
      );
    }
    return result.state;
  }

  private deliveryTallies(
    db: Database.Database,
    endpointId: string | null,
    messageId: string | null,
    now: number,
  ): { unacked: number; recovery: number } {
    const cutoff = toIso(now - PRESENTED_TTL_MS);
    const row = db
      .prepare(
        `SELECT
           COALESCE(SUM(CASE
             WHEN state IN ('pending','leased','presented') THEN 1
             ELSE 0 END), 0) AS unacked,
           COALESCE(SUM(CASE
             WHEN state = 'leased' AND lease_until < ? THEN 1
             ELSE 0 END), 0) AS expired_leased,
           COALESCE(SUM(CASE
             WHEN state = 'presented'
              AND confirmed_at IS NULL
              AND presented_at < ? THEN 1
             ELSE 0 END), 0) AS expired_presented
         FROM deliveries
        WHERE (? IS NULL OR endpoint_id = ?)
          AND (? IS NULL OR message_id = ?)`,
      )
      .get(now, cutoff, endpointId, endpointId, messageId, messageId) as {
      unacked: number;
      expired_leased: number;
      expired_presented: number;
    };
    return {
      unacked: Number(row.unacked),
      recovery: Number(row.expired_leased) + Number(row.expired_presented),
    };
  }

  private peekOn(
    db: Database.Database,
    endpoint: EndpointRow,
    limit: number,
    messageId: string | null,
    cursor: number | null,
    now: number,
  ): FetchResult {
    const page = db
      .prepare(
        `SELECT d.delivery_id AS deliveryId,
                d.attempt_count AS attemptCount,
                d.message_id AS messageId,
                m.subject AS subject,
                m.body AS body,
                src.name AS fromEndpoint
           FROM deliveries d
           JOIN messages m
             ON m.message_id = d.message_id
           LEFT JOIN endpoints src
             ON src.endpoint_id = m.source_endpoint_id
          WHERE d.endpoint_id = ?
            AND d.state = 'pending'
            AND (? IS NULL OR d.message_id = ?)
            AND (? IS NULL OR d.delivery_id > ?)
          ORDER BY d.delivery_id
          LIMIT ?`,
      )
      .all(
        endpoint.endpoint_id,
        messageId,
        messageId,
        cursor,
        cursor,
        limit + 1,
      ) as Array<{
      deliveryId: number;
      attemptCount: number;
      messageId: string;
      subject: string;
      body: string;
      fromEndpoint: string | null;
    }>;
    const rows = page.slice(0, limit);
    const hasMore = page.length > limit;
    const last = rows[rows.length - 1];
    const tallies = this.deliveryTallies(db, endpoint.endpoint_id, null, now);
    return {
      next_cursor: hasMore ? (last?.deliveryId ?? null) : null,
      messages: rows.map((row) => ({
        message_id: row.messageId,
        attempt_id: null,
        subject: row.subject,
        from_endpoint: row.fromEndpoint,
        body_bytes: Buffer.byteLength(row.body, "utf8"),
        redelivery: row.attemptCount > 0,
      })),
      has_more: hasMore,
      unacked_total: tallies.unacked,
      recovery_owed: tallies.recovery,
      peek: true,
    };
  }

  private fetchDeliveries(
    endpoint: EndpointRow,
    consumerInput: string,
    options: {
      peek?: boolean;
      limit?: number;
      now?: number;
      messageId?: unknown;
      cursor?: unknown;
    },
  ): FetchResult {
    const consumer = requireConsumer(consumerInput);
    const peek = options.peek ?? false;
    const messageId =
      options.messageId === undefined || options.messageId === null
        ? null
        : validateMessageId(options.messageId);
    const limit =
      messageId === null
        ? requireLimit(options.limit ?? DEFAULT_FETCH_LIMIT)
        : 1;
    const now = options.now ?? Date.now();
    const cursor =
      options.cursor === undefined || options.cursor === null
        ? null
        : requireCursor(options.cursor);
    if (peek) {
      const opened = openVerifiedDatabase(this.dbPath, true);
      try {
        const read = opened.db.transaction(() =>
          this.peekOn(opened.db, endpoint, limit, messageId, cursor, now),
        );
        return read.deferred();
      } finally {
        opened.db.close();
      }
    }
    if (cursor !== null) {
      throw new BridgeError(
        "cursor is only meaningful with peek: a claim advances the queue by taking rows",
      );
    }
    const run = this.db.transaction(() => {
      this.recoverDeliveries(endpoint.role, now, endpoint.endpoint_id);
      const claimed = this.claimDeliveries(
        endpoint,
        consumer,
        limit,
        now,
        messageId,
      );
      this.presentDeliveries(
        endpoint,
        consumer,
        claimed.map((row) => ({
          messageId: row.messageId,
          attemptId: row.attemptId,
        })),
        now,
      );
      const pending = this.db
        .prepare(
          `SELECT COUNT(*) AS count
             FROM deliveries
            WHERE endpoint_id = ?
              AND state = 'pending'`,
        )
        .get(endpoint.endpoint_id) as { count: number };
      const tallies = this.deliveryTallies(
        this.db,
        endpoint.endpoint_id,
        null,
        now,
      );
      return {
        messages: claimed.map((row) => ({
          message_id: row.messageId,
          attempt_id: row.attemptId,
          subject: row.subject,
          from_endpoint: row.fromEndpoint,
          body_bytes: Buffer.byteLength(row.body, "utf8"),
          body: row.body,
          redelivery: row.attemptCount > 1,
        })),
        has_more: pending.count > 0,
        unacked_total: tallies.unacked,
        peek: false as const,
      };
    });
    return run.immediate();
  }

  private readDeliveryStatus(messageIdInput: unknown): BridgeStatus {
    const messageId = validateMessageId(messageIdInput);
    const message = this.db
      .prepare(
        `SELECT legacy_to_tag, legacy_from_tag,
                envelope_sha256, body_sha256
           FROM messages
          WHERE message_id = ?`,
      )
      .get(messageId) as
      | {
          legacy_to_tag: string | null;
          legacy_from_tag: string | null;
          envelope_sha256: string;
          body_sha256: string;
        }
      | undefined;
    if (!message) {
      throw new BridgeError(`message_id not found: ${messageId}`);
    }
    const deliveries = this.db
      .prepare(
        `SELECT ep.name AS endpoint,
                d.state AS state,
                d.holder AS holder,
                d.attempt_id AS attempt_id,
                d.attempt_count AS attempt_count,
                d.lease_until AS lease_until,
                d.presented_at AS presented_at,
                d.confirmed_at AS confirmed_at
           FROM deliveries d
           JOIN endpoints ep
             ON ep.endpoint_id = d.endpoint_id
          WHERE d.message_id = ?
          ORDER BY d.delivery_id`,
      )
      .all(messageId) as NonNullable<BridgeStatus["deliveries"]>;
    const events = this.db
      .prepare(
        `SELECT me.seq AS seq,
                me.message_id AS message_id,
                me.attempt_id AS attempt_id,
                me.event AS event,
                me.at AS at,
                me.detail AS detail,
                ep.name AS endpoint
           FROM message_events me
           JOIN endpoints ep
             ON ep.endpoint_id = me.endpoint_id
          WHERE me.message_id = ?
          ORDER BY me.seq`,
      )
      .all(messageId) as EventRow[];
    const eventCounts: Record<string, number> = {};
    for (const event of events) {
      eventCounts[event.event] = (eventCounts[event.event] ?? 0) + 1;
    }
    const tallies = this.deliveryTallies(this.db, null, messageId, Date.now());
    return {
      message_id: messageId,
      legacy_to_tag: message.legacy_to_tag,
      legacy_from_tag: message.legacy_from_tag,
      envelope_sha256: message.envelope_sha256,
      body_sha256: message.body_sha256,
      deliveries,
      event_counts: eventCounts,
      events,
      unacked_total: tallies.unacked,
      recovery_owed: tallies.recovery,
    };
  }

  cancelDeliveries(input: {
    messageId: unknown;
    endpointName?: string | null;
    reason: unknown;
    now?: number;
  }): { cancelled: string[] } {
    const messageId = validateMessageId(input.messageId);
    if (typeof input.reason !== "string" || input.reason.trim().length === 0) {
      throw new BridgeError("reason must be a non-empty string");
    }
    const reason = input.reason.trim();
    const endpointName = input.endpointName ?? null;
    const nowIso = toIso(input.now ?? Date.now());
    const operation = this.db.transaction(() => {
      const message = this.db
        .prepare(`SELECT message_id FROM messages WHERE message_id = ?`)
        .get(messageId) as { message_id: string } | undefined;
      if (!message) {
        throw new BridgeError(`message_id not found: ${messageId}`);
      }
      const rows = this.db
        .prepare(
          `SELECT d.delivery_id AS deliveryId,
                  d.state AS state,
                  ep.name AS name
             FROM deliveries d
             JOIN endpoints ep
               ON ep.endpoint_id = d.endpoint_id
            WHERE d.message_id = ?
            ORDER BY d.delivery_id`,
        )
        .all(messageId) as Array<{
        deliveryId: number;
        state: string;
        name: string;
      }>;
      let targets = rows;
      if (endpointName !== null) {
        targets = rows.filter((row) => row.name === endpointName);
        if (targets.length === 0) {
          const known = this.db
            .prepare(`SELECT endpoint_id FROM endpoints WHERE name = ?`)
            .get(endpointName) as { endpoint_id: string } | undefined;
          throw new BridgeError(
            known
              ? `message ${messageId} has no delivery to endpoint ${quoteForOneLine(endpointName)}`
              : `no endpoint named ${quoteForOneLine(endpointName)} is registered`,
          );
        }
        if (targets.length > 1) {
          throw new BridgeError(
            `endpoint name ${quoteForOneLine(endpointName)} matches more than one delivery of ${messageId}`,
          );
        }
      }
      const held = targets.find(
        (row) => row.state === "leased" || row.state === "presented",
      );
      if (held) {
        throw new BridgeError(
          `cannot cancel ${messageId}: delivery to ${held.name} is ${held.state}`,
        );
      }
      const pending = targets.filter((row) => row.state === "pending");
      if (pending.length === 0) {
        throw new BridgeError(`no pending delivery to cancel for ${messageId}`);
      }
      const cancel = this.db.prepare(
        `UPDATE deliveries
            SET state = 'cancelled'
          WHERE delivery_id = ?
            AND state = 'pending'`,
      );
      for (const row of pending) {
        const update = cancel.run(row.deliveryId);
        this.assertOneChange(
          update.changes,
          `pending->cancelled failed for ${messageId}`,
        );
        this.insertEvent(
          row.deliveryId,
          null,
          "cancelled",
          nowIso,
          JSON.stringify({ reason }),
        );
      }
      return pending.map((row) => row.name);
    });
    return { cancelled: operation.immediate() };
  }

  send(input: {
    fromRole: Role;
    toRole: Role;
    subject: unknown;
    body: unknown;
    messageId?: unknown;
    senderThreadId?: unknown;
    toTag?: unknown;
    toEndpoint?: unknown;
    broadcast?: unknown;
    fromTag?: unknown;
    sourceEndpoint?: EndpointRow | null;
    onTimeout?: unknown;
    toEndpoints?: unknown;
    now?: number;
  }): SendResult {
    return this.deliverSend(input);
  }

  recover(roleInput: Role, now = Date.now()): RecoveryResult {
    const role = requireRole(roleInput);
    const operation = this.db.transaction(() =>
      this.recoverWithinTransaction(role, now),
    );
    return operation.immediate();
  }

  private recoverWithinTransaction(role: Role, now: number): RecoveryResult {
    return this.recoverDeliveries(role, now, null);
  }

  claim(
    roleInput: Role,
    consumerInput: string,
    limitInput = DEFAULT_FETCH_LIMIT,
    now = Date.now(),
    sessionTagInput: unknown = null,
    endpointInput: EndpointRow | null = null,
  ): ClaimedMessage[] {
    if (endpointInput === null) {
      throw new BridgeError("claim requires the server endpoint");
    }
    const run = this.db.transaction(() =>
      this.claimDeliveries(
        endpointInput,
        requireConsumer(consumerInput),
        requireLimit(limitInput),
        now,
        null,
      ),
    );
    return run
      .immediate()
      .map((row) => this.asClaimed(row, requireConsumer(consumerInput), now));
  }

  markPresented(
    roleInput: Role,
    consumerInput: string,
    messages: ReadonlyArray<{
      messageId: string;
      attemptId: string;
    }>,
    now = Date.now(),
    endpointInput: EndpointRow | null = null,
  ): void {
    if (messages.length === 0) {
      return;
    }
    if (endpointInput === null) {
      throw new BridgeError("markPresented requires the server endpoint");
    }
    const run = this.db.transaction(() => {
      this.presentDeliveries(endpointInput, consumerInput, messages, now);
    });
    run.immediate();
    return;
  }

  ack(
    roleInput: Role,
    messageIdInput: unknown,
    attemptIdInput: unknown,
    now = Date.now(),
    consumerInput: unknown = undefined,
    endpointInput: EndpointRow | null = null,
  ): LatestMessageState {
    if (endpointInput === null) {
      throw new BridgeError("ack requires the server endpoint");
    }
    return this.confirmDelivery(
      endpointInput,
      messageIdInput,
      attemptIdInput,
      now,
      consumerInput,
    );
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
      cursor?: unknown;
      endpoint?: EndpointRow | null;
    } = {},
  ): FetchResult {
    if (options.endpoint !== undefined && options.endpoint !== null) {
      return this.fetchDeliveries(options.endpoint, consumerInput, options);
    }
    throw new BridgeError("fetch requires the server endpoint");
  }

  status(messageIdInput: unknown): BridgeStatus {
    return this.readDeliveryStatus(messageIdInput);
  }

  readMessage(messageIdInput: unknown): MessageRow | undefined {
    const messageId = validateMessageId(messageIdInput);
    return this.db
      .prepare("SELECT * FROM messages WHERE message_id = ?")
      .get(messageId) as MessageRow | undefined;
  }

  /*
   * A stored row with no `tag_expires_at` is a row no timer will move.
   * That is every untagged row, whose CHECK forbids it a deadline, and
   * since v7 every bounce, which holds its address on purpose. Neither
   * expires and neither bounces, so neither leaves the head of a peek
   * until a session takes it, and each holds a slot of the reachable
   * window for good. A fallback demotion produces the first kind without
   * anyone choosing to, so the pool is reported rather than left to be
   * discovered by a session that can no longer reach past it.
   *
   * The predicate is the negation of the one the sweep expires rows by
   * rather than `to_tag IS NULL`. Those named the same rows only while
   * the CHECK tied a tag to a deadline, and 4.1 unties them.
   */
  backlog(role: Role): BacklogCounts {
    const row = this.db
      .prepare(
        `SELECT COUNT(*) AS stuck,
                MIN(m.sent_at) AS oldest
           FROM deliveries d
           JOIN endpoints ep
             ON ep.endpoint_id = d.endpoint_id
           JOIN messages m
             ON m.message_id = d.message_id
          WHERE ep.role = ?
            AND d.state = 'pending'`,
      )
      .get(role) as {
      stuck: number;
      oldest: string | null;
    };

    return {
      stuck: row.stuck,
      oldestSentAt: row.oldest,
    };
  }

  backlogRows(role: Role, limit: number): BacklogRow[] {
    return this.db
      .prepare(
        `SELECT src.name AS from_endpoint,
                m.sent_at AS sent_at
           FROM deliveries d
           JOIN endpoints ep
             ON ep.endpoint_id = d.endpoint_id
           JOIN messages m
             ON m.message_id = d.message_id
           LEFT JOIN endpoints src
             ON src.endpoint_id = m.source_endpoint_id
          WHERE ep.role = ?
            AND d.state = 'pending'
          ORDER BY m.sent_at ASC, d.delivery_id ASC
          LIMIT ?`,
      )
      .all(role, limit) as BacklogRow[];
  }

  /*
   * What a person needs to know, from rows that already hold it. A bounce
   * notification carries neither the original subject nor its destination,
   * but the message it is about keeps both, so the loss is describable
   * without changing what a bounce stores.
   *
   * The window is whatever the caller has already reported. An earlier
   * version asked instead whether the bounce notice was still unacked,
   * which reads well and measures the wrong thing: run against the real
   * database, all six bounces were acked by an agent and this returned
   * nothing, while the person had still found them by counting rows.
   *
   * The page is cut in SQL rather than after loading. Nothing prunes
   * messages or events, so a slice taken in memory grows with the whole
   * history of the deployment and the sweep pays for rows it discards.
   */
  undelivered(
    role: Role,
    since: number | null,
    limit: number,
  ): UndeliveredReport {
    const sql = lostQuerySql();
    const from = since ?? 0;

    const lost = this.db.prepare(sql.page).all({
      role,
      since: from,
      limit,
    }) as UndeliveredMessage[];

    const lostSince = (
      this.db.prepare(sql.count).get({
        role,
        since: from,
      }) as {
        count: number;
      }
    ).count;

    const lostTotal = (
      this.db
        .prepare(
          `SELECT COUNT(*) AS count
             FROM deliveries d
             JOIN endpoints ep
               ON ep.endpoint_id = d.endpoint_id
            WHERE ep.role = @role
              AND d.state = 'bounced'`,
        )
        .get({ role }) as { count: number }
    ).count;

    return { lost, lostSince, lostTotal };
  }

  /*
   * Read the page and move the cursor past it in one write transaction, so
   * two sweeps cannot both take the same rows. Without this they read the
   * same cursor, both print, and one advances: the second run announces
   * rows the first already named, which is the double counting the heading
   * was renamed to avoid. Two sweeps ran one second apart in this
   * deployment today, so it is not a theoretical overlap.
   *
   * Reserving before printing means a run that dies in between leaves its
   * rows unnamed. They stay in the running total, which is what keeps that
   * total in the output at all.
   */
  reserveLosses(role: Role, limit: number): UndeliveredReport {
    const reserve = this.db.transaction((): UndeliveredReport => {
      const since = this.readSweepMark(role);
      const report = this.undelivered(role, since, limit);
      const last = report.lost[report.lost.length - 1]?.seq;

      this.writeCursor(role, last ?? since ?? 0);
      return report;
    });

    return reserve.immediate();
  }

  /*
   * How far the reporting has got, so "what failed since you last looked"
   * needs no age window anyone had to choose.
   */
  readSweepMark(role: Role): number | null {
    const row = this.db
      .prepare("SELECT v FROM meta WHERE k = ?")
      .get(sweepCursorKey(role)) as { v: string } | undefined;

    return row === undefined ? null : Number(row.v);
  }

  /*
   * Two keys, because one answered two questions. The cursor says how far
   * the reporting reached; the completion stamp says the sweep ran at all.
   * Sharing a key left a stopped sweep and a quiet one both silent, with
   * nothing able to tell them apart.
   *
   * The cursor only moves forward. Two sweeps at once both read the older
   * value, and an unconditional write lets the slower one pull it back
   * over ground the other already reported. Repeating a loss is fine;
   * stepping over an unscanned stretch is not.
   */
  /*
   * Compared as an integer. Stored as text like every other meta value,
   * and "10" sorts before "9" as text, so a lexicographic guard would
   * refuse every cursor past the first nine events.
   */
  private writeCursor(role: Role, cursor: number): void {
    this.db
      .prepare(
        `INSERT INTO meta (k, v) VALUES (@key, @cursor)
           ON CONFLICT(k) DO UPDATE SET v = @cursor
            WHERE meta.v IS NULL
               OR CAST(meta.v AS INTEGER) < @cursor`,
      )
      .run({
        key: sweepCursorKey(role),
        cursor,
      });
  }

  writeSweepMark(role: Role, cursor: number, now = Date.now()): void {
    this.writeCursor(role, cursor);
    this.markSweepCompleted(now);
  }

  /*
   * Guarded like the cursor, and for the same overlap. A run that started
   * earlier can finish later, and an unguarded write puts its older stamp
   * on top, so anything watching for a stopped sweep reads a staleness
   * that never happened.
   */
  markSweepCompleted(now = Date.now()): void {
    this.db
      .prepare(
        `INSERT INTO meta (k, v) VALUES ('sweep_last_completed', @at)
           ON CONFLICT(k) DO UPDATE SET v = @at
            WHERE meta.v IS NULL OR meta.v < @at`,
      )
      .run({ at: toIso(now) });
  }

  readSweepCompletedAt(): string | null {
    const row = this.db
      .prepare("SELECT v FROM meta WHERE k = ?")
      .get("sweep_last_completed") as { v: string } | undefined;

    return row?.v ?? null;
  }

  private insertEvent(
    deliveryId: number,
    attemptId: string | null,
    event: string,
    at: string,
    detail: string | null,
  ): void {
    this.db
      .prepare(
        `INSERT INTO events (
           delivery_id,
           attempt_id,
           event,
           at,
           detail
         ) VALUES (?, ?, ?, ?, ?)`,
      )
      .run(deliveryId, attemptId, event, at, detail);
  }

  private assertOneChange(changes: number, message: string): void {
    if (changes !== 1) {
      throw new BridgeTransitionError(message, null);
    }
  }
}
