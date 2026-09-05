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
import { quoteForOneLine } from "./one-line.js";

export const LEGACY_SCHEMA_VERSION = "3.2";
export const SCHEMA_VERSION = "4.9";

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
  rows: (db: Database.Database) => void;
}

export type MigrationStep =
  | RebuildMigrationStep
  | DdlMigrationStep
  | FillMigrationStep;
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
}

export interface RefusedSendResult {
  kind: "refused";
  reason: "second_delivery_before_stage4";
}

export type SendResult =
  | StoredSendResult
  | RefusedSendResult;

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
  from_tag: string | null;
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
  const window = `
           FROM messages m
           JOIN events e
             ON e.message_id = m.message_id
            AND e.event = 'bounced'
          WHERE m.to_role = @role
            AND m.status = 'bounced'
            AND e.seq > @since`;

  /*
   * from_tag, not to_tag. The row this report is about has already
   * bounced, and the bounce the sweep wrote for it is addressed to the
   * sender's lane; to_tag names the lane that did not answer, which is
   * the one address that is certainly unreachable. Printing that sent
   * the operator to declare a dead tag and fetch nothing, while the only
   * row a person can still act on sat under a name the report never
   * showed. Both are printed now, in the order they are useful.
   */
  return {
    page: `SELECT m.subject  AS subject,
                m.from_tag AS bounceToTag,
                m.to_tag   AS deadTag,
                e.at       AS at,
                e.seq      AS seq
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

function createMessagesTableSql(
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

const DELIVERIES_TABLE_SQL = `
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

const MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL = `
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

/*
 * A table and the trigger that guards it, in the two groups the ladder
 * puts on either side of the rebuild. Held here rather than spelled into
 * the steps so a test standing in a wrong implementation builds on the
 * same statements the real one runs.
 */
export const STAGE_ONE_ENDPOINTS_SQL: readonly string[] =
  [
    ENDPOINTS_TABLE_SQL,
    ENDPOINTS_IMMUTABLE_TRIGGER_SQL,
  ];

export const STAGE_ONE_DELIVERIES_SQL: readonly string[] =
  [
    DELIVERIES_TABLE_SQL_4_6,
    DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
    DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL_4_6,
  ];

export const SCHEMA_SQL = `
CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
${ENDPOINTS_TABLE_SQL}
${createMessagesTableSql("messages")}

CREATE INDEX idx_inbox
  ON messages (to_role, status, id);
${DELIVERIES_TABLE_SQL}
${DELIVERIES_ONE_PER_MESSAGE_INDEX_SQL}
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT,
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
${ENDPOINTS_IMMUTABLE_TRIGGER_SQL}${DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL}${DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL}${DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL}${MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL}`;

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const UUID_RFC_4122 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function assertRootId(
  value: unknown,
  where: "root_id" | "meta.root_id",
): void {
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

/*
 * One destination predicate, shared by every place that decides what a
 * session may see. It lived as five copies of the same SQL plus a sixth
 * shape in the hook, which is a machine for making them disagree: the
 * next change to the semantics would have had to find all six.
 *
 * Indented per call site so the surrounding statements stay readable.
 */
/*
 * The three shapes recovery stages, defined once. countRecoveryOwed reports
 * what the sweep is going to move, so a second copy of these predicates
 * would let the report and the sweep disagree without either being wrong on
 * its own. This file already learned that with the destination predicate.
 *
 * "Once" reaches as far as this module. It does not reach src/hook-notify.ts,
 * which spells the same three shapes out again in its own SQL because it
 * runs in a separate process on a read-only connection and splits them into
 * buckets a notice can name rather than summing them. That copy is correct
 * today -- it discards NULL deadlines the same way these do -- and it is
 * still a copy: changing the meaning of any of the three means changing it
 * there too. An earlier version of this comment implied otherwise, and a
 * reviewer looking only where the constants are used concluded there was no
 * fourth site.
 */
export const EXPIRED_CLAIM_SQL = `status = 'claimed'
            AND lease_expires_at < @now`;

export const STALE_PRESENTED_SQL = `status = 'presented'
            AND acked_at IS NULL
            AND presented_at < @presentedCutoff`;

/*
 * `tag_expires_at IS NOT NULL` is not redundant with the comparison below
 * it. Since v7 a tag can be held with no deadline, and `NULL < @now` is
 * NULL, not false. Two of this predicate's three call sites in this file
 * survive that:
 * the sweep selects on it, where NULL is discarded, and the recovery count
 * ORs it, where NULL is discarded too. The third negates it, and `NOT NULL`
 * is NULL, so a bounce -- the one row that holds a tag without a deadline --
 * vanished from every peek while still sitting stored in the inbox. A
 * session learns message_ids by peeking, so the addressee could never name
 * the row to fetch it, and the notice that a message had not arrived did
 * not arrive either.
 *
 * Stated here rather than at the negation, so the predicate means the same
 * thing under NOT as it does under SELECT: a row whose tag has run out. A
 * row with no deadline has not run out, it is unexpiring.
 */
export const EXPIRED_TAGGED_SQL = `status = 'stored'
            AND to_tag IS NOT NULL
            AND tag_expires_at IS NOT NULL
            AND tag_expires_at < @now`;

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
  return sha256(
    JSON.stringify([
      2,
      fromRole,
      subject,
      body,
      null,
      null,
      0,
    ]),
  );
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
export const DECLARED_TAG_ENV =
  "AGENT_BRIDGE_TAG";

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

  if (
    raw === undefined ||
    raw.trim().length === 0
  ) {
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
        error instanceof Error
          ? error.message
          : String(error)
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
): MigrationStep[] {
  const planned: MigrationStep[] = [];
  let at = from;

  while (at !== SCHEMA_VERSION) {
    const step = steps.find(
      (candidate) => candidate.from === at,
    );

    if (!step) {
      throw new BridgeDatabaseError(
        `no migration path from schema_version ${at} to ${SCHEMA_VERSION}; the versions that can be migrated from are ${steps
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

function copyLegacyRows(
  db: Database.Database,
  staging: string,
): void {
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
function copyNamedColumns(
  columns: string,
): MigrationCopy {
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
${DELIVERIES_TABLE_SQL}
${DELIVERIES_ONE_PER_MESSAGE_INDEX_SQL}
${DELIVERIES_IDENTITY_IMMUTABLE_TRIGGER_SQL}
`;

function copyStageTwoRows(
  db: Database.Database,
  staging: string,
): void {
  const rows = db
    .prepare(
      `SELECT id,
              from_role,
              subject,
              body
         FROM messages
        ORDER BY id`,
    )
    .all() as Array<
    Pick<
      MessageRow,
      "id" | "from_role" | "subject" | "body"
    >
  >;

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

function fillDeliveries(
  db: Database.Database,
): void {
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

function rebuildMessages(
  from: string,
  to: string,
  stagingSql: string,
  copy: MigrationCopy,
  after: readonly string[] = [
    MESSAGES_INBOX_INDEX_SQL,
  ],
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

export const MIGRATION_STEPS: readonly MigrationStep[] =
  [
    rebuildMessages(
      LEGACY_SCHEMA_VERSION,
      "4.0",
      MESSAGES_STAGING_SQL_4_2,
      {
        via: "rows",
        rows: copyLegacyRows,
      },
    ),
    rebuildMessages(
      "4.0",
      "4.1",
      MESSAGES_STAGING_SQL_4_2,
      COPY_EVERY_COLUMN,
    ),
    rebuildMessages(
      "4.1",
      "4.2",
      MESSAGES_STAGING_SQL_4_2,
      COPY_EVERY_COLUMN,
    ),
    rebuildMessages(
      "4.2",
      "4.3",
      MESSAGES_STAGING_SQL_4_3,
      COPY_WITHOUT_ROOT_ID,
    ),
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
      createMessagesTableSql(
        MIGRATION_STAGING_TABLE,
        false,
      ),
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
      statements: [
        STAGE_TWO_DELIVERIES_SQL,
      ],
    },
    rebuildMessages(
      "4.7",
      "4.8",
      createMessagesTableSql(
        MIGRATION_STAGING_TABLE,
      ),
      {
        via: "rows",
        rows: copyStageTwoRows,
      },
      [
        MESSAGES_INBOX_INDEX_SQL,
        DELIVERIES_ROLE_DIFFERS_TRIGGER_SQL,
        DELIVERIES_ROLE_DIFFERS_ON_ASSIGN_TRIGGER_SQL,
        MESSAGES_IDENTITY_IMMUTABLE_TRIGGER_SQL,
      ],
    ),
    {
      kind: "fill",
      from: "4.8",
      to: SCHEMA_VERSION,
      rows: fillDeliveries,
    },
  ];

function rowCount(
  db: Database.Database,
  table: string,
): number {
  return (
    db
      .prepare(
        `SELECT COUNT(*) AS count FROM ${table}`,
      )
      .get() as { count: number }
  ).count;
}

function rebuildStepTable(
  db: Database.Database,
  step: RebuildMigrationStep,
  options: MigrationOptions,
): void {
  db.exec(step.stagingSql);

  const sourceCount = rowCount(
    db,
    step.table,
  );

  if (step.copy.via === "sql") {
    db.exec(step.copy.sql);
  } else {
    step.copy.rows(db, step.staging);
  }

  const copiedCount = rowCount(
    db,
    step.staging,
  );

  if (copiedCount !== sourceCount) {
    throw new BridgeDatabaseError(
      `migration row-count mismatch: source=${sourceCount} copied=${copiedCount}`,
    );
  }

  db.exec(`
DROP TABLE ${step.table};
ALTER TABLE ${step.staging} RENAME TO ${step.table};
`);

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
    step.rows(db);
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
    throw new BridgeDatabaseError(
      "schema_version changed during migration",
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

      if (!schema?.v) {
        throw new BridgeDatabaseError(
          "meta.schema_version is missing",
        );
      }

      if (!root?.v) {
        throw new BridgeDatabaseError(
          "meta.root_id is missing",
        );
      }

      assertRootId(root.v, "meta.root_id");

      const planned = planMigration(
        schema.v,
        steps,
      );

      if (planned.length === 0) {
        throw new BridgeDatabaseError(
          `schema_version is already ${SCHEMA_VERSION}; there is nothing to migrate`,
        );
      }

      for (const step of planned) {
        applyMigrationStep(
          db,
          step,
          options,
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

    assertRootId(root.v, "meta.root_id");

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

function requireCursor(value: unknown): number {
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < 1
  ) {
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

  /*
   * The only writer of this table. A server that meets a name it does not
   * know rejects the startup instead of adding the row, so a name reaches
   * the registry through an operator running `bridge-init
   * --add-endpoint` and no other way.
   */
  addEndpoint(
    role: Role,
    name: string,
    now = new Date(),
  ): EndpointRow {
    const endpointName =
      typeof name === "string"
        ? name
        : "";

    if (
      endpointName.trim().length === 0
    ) {
      throw new BridgeError(
        "endpoint name must be a non-empty string",
      );
    }

    /*
     * The ceiling `normalizeTag` puts on the other address an operator
     * types by hand, counted in the same UTF-8 bytes, because an endpoint
     * name is the same kind of value and a second number would only be a
     * second thing to remember. First of the three refusals, so the two
     * below quote the name back at a length someone can read.
     */
    const nameBytes = Buffer.byteLength(
      endpointName,
      "utf8",
    );

    if (nameBytes > 200) {
      throw new BridgeError(
        `endpoint name is ${nameBytes} UTF-8 bytes; register a name of 200 bytes or fewer`,
      );
    }

    /*
     * Refused rather than repaired, in this check and the next, because
     * `resolveEndpoint` compares the `--endpoint` argument as it arrives:
     * a row whose stored name is not the name the operator typed is a row
     * no server can select. A control character earns the refusal twice
     * over. `bridge-init` answers a registration with a one-line record
     * and the server writes the name into the startup line `docs/deploy.md`
     * tells an operator to read, and a record ends where the newline is,
     * so a name holding one composes a second record underneath that
     * nothing marks as having come from the name. U+2028 and U+2029 end
     * a record the same way for every reader that breaks lines as
     * Python's `str.splitlines()` does, and neither is a control
     * character nor whitespace `trim` takes, so the class names them
     * beside the ones a terminal would have swallowed.
     */
    if (
      /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/.test(
        endpointName,
      )
    ) {
      throw new BridgeError(
        `endpoint name ${quoteForOneLine(
          endpointName,
        )} holds a control character; register a name that prints as the one line it is written on`,
      );
    }

    if (
      endpointName !== endpointName.trim()
    ) {
      throw new BridgeError(
        `endpoint name ${quoteForOneLine(
          endpointName,
        )} is padded with whitespace; register the name exactly as --endpoint will be given it`,
      );
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
        .get(row.role, row.name) as
        | { endpoint_id: string }
        | undefined;

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
        .run(
          row.endpoint_id,
          row.role,
          row.name,
          row.created_at,
        );
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
  resolveEndpoint(
    role: Role,
    name: string,
  ): EndpointRow {
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

    const mine = rows.find(
      (row) => row.role === role,
    );

    if (!mine) {
      throw new BridgeError(
        rows.length === 0
          ? `no endpoint named ${quoteForOneLine(name)} is registered; add it with bridge-init --add-endpoint`
          : `endpoint ${quoteForOneLine(name)} is registered for ${rows
              .map((row) => row.role)
              .sort()
              .join(
                ",",
              )}, not ${role}`,
      );
    }

    if (mine.retired_at !== null) {
      throw new BridgeError(
        `endpoint ${role}/${name} was retired at ${mine.retired_at}`,
      );
    }

    return mine;
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
    toEndpoint?: unknown;
    broadcast?: unknown;
    fromTag?: unknown;
    sourceEndpoint?: EndpointRow | null;
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
    const sourceEndpoint =
      input.sourceEndpoint ?? null;

    if (
      sourceEndpoint !== null &&
      sourceEndpoint.role !== fromRole
    ) {
      throw new BridgeError(
        "source endpoint role does not match from_role",
      );
    }

    let toEndpointName: string | null = null;
    if (
      input.toEndpoint !== undefined &&
      input.toEndpoint !== null
    ) {
      if (typeof input.toEndpoint !== "string") {
        throw new BridgeError(
          "to_endpoint must be a string when provided",
        );
      }

      toEndpointName = input.toEndpoint;
    }

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

    if (
      toEndpointName !== null &&
      toTag !== null
    ) {
      throw new BridgeError(
        "conflicting_destination: to_endpoint and to_tag cannot both address one message",
      );
    }

    if (toEndpointName !== null && broadcast) {
      throw new BridgeError(
        "conflicting_destination: to_endpoint and broadcast cannot both address one message",
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

    const envelopeHash = envelopeHashSeam.compute(
      fromRole,
      subject,
      body,
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
          kind: "refused";
          reason: "second_delivery_before_stage4";
        }
      | {
          kind: "conflict";
          existingHash: string;
          senderMismatch: boolean;
        };

    const operation = this.db.transaction(
      (): TransactionResult => {
        const existing = this.db
          .prepare(
            `SELECT envelope_sha256,
                    source_endpoint_id,
                    from_tag,
                    to_role,
                    legacy_to_tag
               FROM messages
              WHERE message_id = ?`,
          )
          .get(messageId) as
          | {
              envelope_sha256: string;
              source_endpoint_id: string | null;
              from_tag: string | null;
              to_role: Role;
              legacy_to_tag: string | null;
            }
          | undefined;

        if (existing) {
          const senderMatches =
            existing.source_endpoint_id === null
              ? sourceEndpoint === null &&
                existing.from_tag === fromTag
              : sourceEndpoint?.endpoint_id ===
                existing.source_endpoint_id;

          if (!senderMatches) {
            this.insertEvent(
              messageId,
              null,
              "send_conflict",
              sentAt,
              JSON.stringify({
                sender_mismatch: true,
              }),
            );

            return {
              kind: "conflict",
              existingHash:
                existing.envelope_sha256,
              senderMismatch: true,
            };
          }

          if (
            existing.envelope_sha256 !==
            envelopeHash
          ) {
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
              senderMismatch: false,
            };
          }

          const requestedEndpoint =
            toEndpointName === null
              ? null
              : (this.db
                  .prepare(
                    `SELECT endpoint_id
                       FROM endpoints
                      WHERE role = ?
                        AND name = ?`,
                  )
                  .get(
                    toRole,
                    toEndpointName,
                  ) as
                  | { endpoint_id: string }
                  | undefined);
          const delivery = this.db
            .prepare(
              `SELECT endpoint_id
                 FROM deliveries
                WHERE message_id = ?`,
            )
            .get(messageId) as
            | { endpoint_id: string | null }
            | undefined;

          const sameDestination =
            delivery !== undefined &&
            (delivery.endpoint_id === null
              ? requestedEndpoint === null &&
                existing.to_role === toRole &&
                existing.legacy_to_tag === toTag
              : requestedEndpoint?.endpoint_id ===
                delivery.endpoint_id);

          if (sameDestination) {
            return { kind: "idempotent" };
          }

          const reason =
            "second_delivery_before_stage4" as const;

          this.insertEvent(
            messageId,
            null,
            "send_refused",
            sentAt,
            JSON.stringify({ reason }),
          );

          return {
            kind: "refused",
            reason,
          };
        }

        const destinationEndpoint =
          toEndpointName === null
            ? null
            : this.resolveEndpoint(
                toRole,
                toEndpointName,
              );

        /*
         * The policy is read here rather than at open, so enabling it
         * reaches servers that are already running. The read and all
         * inserts share this transaction.
         */
        const requiredRoles =
          this.readPolicyRoles("require_tag");

        destinationRequiresTag =
          requiredRoles.has(toRole);

        if (
          requiredRoles.has(toRole) &&
          toEndpointName === null &&
          toTag === null &&
          !broadcast
        ) {
          throw new BridgeError(
            "tag_required: address it with to_tag, or set broadcast: true to mean the whole role",
          );
        }

        /*
         * `fallback` opens the row to the whole destination role once the
         * tag expires, which is the reach of `broadcast` arriving half an
         * hour late. The send-time gate is the only place the policy is
         * read, and the demotion happens in the sweep, so a deployment
         * that demands an address is otherwise walked around by a timer.
         * Role-wide is still available, said at send time rather than by
         * waiting.
         *
         * Only the destination role is consulted: the demotion happens in
         * that role's inbox, and the sender's policy has nothing to say
         * about who may read there.
         */
        if (
          requiredRoles.has(toRole) &&
          onTimeout === "fallback"
        ) {
          throw new BridgeError(
            "fallback_not_allowed: on_timeout: fallback would hand this to the whole role once the tag expires; keep the address with on_timeout: bounce, or drop to_tag and set broadcast: true",
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
          sourceEndpoint === null &&
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

        this.db
          .prepare(
            `INSERT INTO messages (
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
               sent_at,
               source_endpoint_id,
               legacy_to_tag
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
               2,
               ?,
               ?,
               'stored',
               ?,
               ?,
               ?
             )`,
          )
          .run(
            messageId,
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
            sourceEndpoint?.endpoint_id ?? null,
            toTag,
          );

        this.db
          .prepare(
            `INSERT INTO deliveries (
               message_id,
               endpoint_id,
               state
             ) VALUES (?, ?, 'pending')`,
          )
          .run(
            messageId,
            destinationEndpoint?.endpoint_id ??
              null,
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
        result.senderMismatch
          ? `message_id ${messageId} belongs to a different sender`
          : `message_id ${messageId} already exists with a different envelope`,
      );
    }

    if (result.kind === "refused") {
      return result;
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
          WHERE to_role = @role
            AND ${EXPIRED_CLAIM_SQL}
          ORDER BY id`,
      )
      .all({ role, now }) as Array<{
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

      const deliveryUpdate = this.db
        .prepare(
          `UPDATE deliveries
              SET state = 'pending',
                  holder = NULL,
                  attempt_id = NULL,
                  lease_until = NULL
            WHERE message_id = ?
              AND state = 'leased'`,
        )
        .run(row.message_id);

      this.assertOneChange(
        deliveryUpdate.changes,
        `leased->pending delivery recovery failed for ${row.message_id}`,
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
          WHERE to_role = @role
            AND ${STALE_PRESENTED_SQL}
          ORDER BY id`,
      )
      .all({ role, presentedCutoff }) as Array<{
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

      const deliveryUpdate = this.db
        .prepare(
          `UPDATE deliveries
              SET state = 'pending',
                  holder = NULL,
                  attempt_id = NULL,
                  lease_until = NULL,
                  presented_at = NULL
            WHERE message_id = ?
              AND state = 'presented'`,
        )
        .run(row.message_id);

      this.assertOneChange(
        deliveryUpdate.changes,
        `presented->pending delivery recovery failed for ${row.message_id}`,
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
          WHERE to_role = @role
            AND ${EXPIRED_TAGGED_SQL}
          ORDER BY id`,
      )
      .all({ role, now }) as MessageRow[];

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

      const deliveryUpdate = this.db
        .prepare(
          `UPDATE deliveries
              SET state = 'bounced'
            WHERE message_id = ?
              AND state = 'pending'`,
        )
        .run(row.message_id);

      this.assertOneChange(
        deliveryUpdate.changes,
        `pending->bounced delivery update failed for ${row.message_id}`,
      );

      const bounceMessageId =
        deriveBounceMessageId(row.message_id);
      const bounceBody =
        `${BOUNCE_REASON}; ` +
        `original_message_id=${row.message_id}`;
      const bounceToTag = row.from_tag;
      /*
       * Addressed, and with no deadline. A bounce used to carry
       * `fallback` plus a TTL, which is what kept a bounce from bouncing,
       * and the price was that thirty minutes later the notice opened to
       * the whole sending role: the message saying nothing arrived was
       * itself no longer guaranteed to arrive. Holding the tag with no
       * deadline keeps both halves. EXPIRED_TAGGED_SQL requires a deadline
       * before it compares one, so a row without one is never picked by
       * the sweep, and the chain `fallback` was avoiding cannot start --
       * and, because the requirement is written out rather than left to
       * NULL, the same row is still visible where that predicate is
       * negated.
       */
      const bounceOnTimeout: TimeoutPolicy | null =
        null;
      const bounceTagExpiresAt: number | null =
        null;
      const bounceEnvelopeHash =
        envelopeHashSeam.compute(
          row.to_role,
          BOUNCE_SUBJECT,
          bounceBody,
        );

      const existingBounce = this.db
        .prepare(
          `SELECT m.envelope_sha256,
                  m.to_role,
                  m.legacy_to_tag,
                  m.from_role,
                  m.from_tag,
                  m.source_endpoint_id,
                  d.endpoint_id
             FROM messages AS m
             JOIN deliveries AS d
               ON d.message_id = m.message_id
            WHERE m.message_id = ?`,
        )
        .get(bounceMessageId) as
        | {
            envelope_sha256: string;
            to_role: Role;
            legacy_to_tag: string | null;
            from_role: Role;
            from_tag: string | null;
            source_endpoint_id: string | null;
            endpoint_id: string | null;
          }
        | undefined;

      let insertedBounce = false;
      if (existingBounce) {
        if (
          existingBounce.envelope_sha256 !==
            bounceEnvelopeHash ||
          existingBounce.to_role !== row.from_role ||
          existingBounce.legacy_to_tag !==
            bounceToTag ||
          existingBounce.endpoint_id !==
            row.source_endpoint_id ||
          existingBounce.from_role !== row.to_role ||
          existingBounce.from_tag !== null ||
          existingBounce.source_endpoint_id !== null
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
               sent_at,
               source_endpoint_id,
               legacy_to_tag
             ) VALUES (
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
               2,
               ?,
               NULL,
               'stored',
               ?,
               NULL,
               ?
             )`,
          )
          .run(
            bounceMessageId,
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
            bounceToTag,
          );

        this.db
          .prepare(
            `INSERT INTO deliveries (
               message_id,
               endpoint_id,
               state
             ) VALUES (?, ?, 'pending')`,
          )
          .run(
            bounceMessageId,
            row.source_endpoint_id,
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

      const deliveryUpdate = this.db
        .prepare(
          `UPDATE deliveries
              SET state = 'leased',
                  holder = ?,
                  attempt_id = ?,
                  attempt_count = attempt_count + 1,
                  lease_until = ?,
                  presented_at = NULL
            WHERE message_id = ?
              AND state = 'pending'`,
        )
        .run(
          consumer,
          attemptId,
          leaseExpiresAt,
          row.message_id,
        );

      this.assertOneChange(
        deliveryUpdate.changes,
        `pending->leased delivery update failed for ${row.message_id}`,
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

        const deliveryRejection = this.db
          .prepare(
            `UPDATE deliveries
                SET state = 'rejected',
                    lease_until = NULL
              WHERE message_id = ?
                AND state = 'leased'
                AND attempt_id = ?
                AND holder = ?`,
          )
          .run(
            row.message_id,
            attemptId,
            consumer,
          );

        this.assertOneChange(
          deliveryRejection.changes,
          `leased->rejected delivery update failed for ${row.message_id}`,
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

        const deliveryUpdate = this.db
          .prepare(
            `UPDATE deliveries
                SET state = 'presented',
                    presented_at = ?,
                    lease_until = NULL
              WHERE message_id = ?
                AND state = 'leased'
                AND attempt_id = ?
                AND holder = ?`,
          )
          .run(
            presentedAt,
            messageId,
            attemptId,
            consumer,
          );

        this.assertOneChange(
          deliveryUpdate.changes,
          `leased->presented delivery update failed for ${messageId}`,
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

        const deliveryUpdate = this.db
          .prepare(
            `UPDATE deliveries
                SET state = 'confirmed',
                    confirmed_at = ?
              WHERE message_id = ?
                AND state = 'presented'
                AND attempt_id = ?
                AND holder = ?`,
          )
          .run(
            ackedAt,
            messageId,
            attemptId,
            consumer,
          );

        this.assertOneChange(
          deliveryUpdate.changes,
          `presented->confirmed delivery update failed for ${messageId}`,
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
      cursor?: unknown;
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
    const cursor =
      options.cursor === undefined ||
      options.cursor === null
        ? null
        : requireCursor(options.cursor);

    if (peek) {
      return this.peek(
        role,
        limit,
        sessionTag,
        messageId,
        this.strictFor(role),
        cursor,
        now,
      );
    }

    if (cursor !== null) {
      throw new BridgeError(
        "cursor is only meaningful with peek: a claim advances the queue by taking rows",
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
                MIN(sent_at) AS oldest
           FROM messages
          WHERE to_role = ?
            AND status = 'stored'
            AND tag_expires_at IS NULL`,
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

  backlogRows(
    role: Role,
    limit: number,
  ): BacklogRow[] {
    return this.db
      .prepare(
        `SELECT from_tag, sent_at
           FROM messages
          WHERE to_role = ?
            AND status = 'stored'
            AND tag_expires_at IS NULL
          ORDER BY sent_at ASC, message_id ASC
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

    const lost = this.db
      .prepare(sql.page)
      .all({
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
             FROM messages
            WHERE to_role = @role
              AND status = 'bounced'`,
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
  reserveLosses(
    role: Role,
    limit: number,
  ): UndeliveredReport {
    const reserve = this.db.transaction(
      (): UndeliveredReport => {
        const since = this.readSweepMark(role);
        const report = this.undelivered(
          role,
          since,
          limit,
        );
        const last =
          report.lost[report.lost.length - 1]
            ?.seq;

        this.writeCursor(
          role,
          last ?? since ?? 0,
        );
        return report;
      },
    );

    return reserve.immediate();
  }

  /*
   * How far the reporting has got, so "what failed since you last looked"
   * needs no age window anyone had to choose.
   */
  readSweepMark(role: Role): number | null {
    const row = this.db
      .prepare(
        "SELECT v FROM meta WHERE k = ?",
      )
      .get(sweepCursorKey(role)) as
      | { v: string }
      | undefined;

    return row === undefined
      ? null
      : Number(row.v);
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
  private writeCursor(
    role: Role,
    cursor: number,
  ): void {
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

  writeSweepMark(
    role: Role,
    cursor: number,
    now = Date.now(),
  ): void {
    this.writeCursor(role, cursor);
    this.markSweepCompleted(now);
  }

  /*
   * Guarded like the cursor, and for the same overlap. A run that started
   * earlier can finish later, and an unguarded write puts its older stamp
   * on top, so anything watching for a stopped sweep reads a staleness
   * that never happened.
   */
  markSweepCompleted(
    now = Date.now(),
  ): void {
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
      .prepare(
        "SELECT v FROM meta WHERE k = ?",
      )
      .get("sweep_last_completed") as
      | { v: string }
      | undefined;

    return row?.v ?? null;
  }

  private peek(
    role: Role,
    limit: number,
    sessionTag: string | null,
    messageId: string | null = null,
    strict = false,
    cursor: number | null = null,
    now = Date.now(),
  ): FetchResult {
    const opened = openVerifiedDatabase(
      this.dbPath,
      true,
    );

    try {
      /*
       * The page and both counts come from one deferred read, so a write
       * landing between them cannot produce a reply describing two
       * different moments. A reader decides what to do from all three
       * together, so they have to be answers about the same instant.
       */
      const read = opened.db.transaction(
        (): FetchResult =>
          this.peekWithinTransaction(
            opened.db,
            role,
            limit,
            sessionTag,
            messageId,
            strict,
            cursor,
            now,
          ),
      );

      return read.deferred();
    } finally {
      opened.db.close();
    }
  }

  private peekWithinTransaction(
    db: Database.Database,
    role: Role,
    limit: number,
    sessionTag: string | null,
    messageId: string | null,
    strict: boolean,
    cursor: number | null,
    now: number,
  ): FetchResult {
    {
      /*
       * One past the page, so "is there more" is answered by what this
       * query saw rather than by a separate count that a cursor would
       * make meaningless.
       */
      const page = db
        .prepare(
          `SELECT *
             FROM messages
            WHERE to_role = @role
              AND status = 'stored'
              AND (
                @messageId IS NULL
                OR message_id = @messageId
              )
              AND (
                @cursor IS NULL
                OR id > @cursor
              )
              AND ${visibleToTagSql(
                "              ",
                strict,
              )}
              /*
               * A row past its tag is still stored and still carries the
               * tag, so its addressee saw it here, judged it as its own,
               * and fetched it. The fetch recovers before it claims, so
               * the row bounced and the reply came back empty. Peek is
               * for deciding what to take, so it shows what a fetch can
               * still deliver.
               */
              AND NOT (${EXPIRED_TAGGED_SQL})
            ORDER BY id
            LIMIT @lookahead`,
        )
        .all({
          role,
          tag: sessionTag,
          lookahead: limit + 1,
          messageId,
          cursor,
          now,
        }) as MessageRow[];

      const rows = page.slice(0, limit);
      const hasMore = page.length > limit;
      const last = rows[rows.length - 1];

      return {
        declared_tag: sessionTag,
        next_cursor: hasMore
          ? (last?.id ?? null)
          : null,
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
        has_more: hasMore,
        unacked_total: this.countUnacked(
          db,
          role,
          sessionTag,
          strict,
        ),
        recovery_owed: this.countRecoveryOwed(
          db,
          role,
          now,
        ),
        peek: true,
      };
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

  /*
   * The three shapes recovery stages, counted role-wide because none of
   * them is visible to a session: a lease past its end, a presentation
   * past its TTL, and a tagged row past its tag. A live claim held by
   * another consumer is not here, which is the whole point.
   */
  private countRecoveryOwed(
    db: Database.Database,
    role: Role,
    now: number,
  ): number {
    const presentedCutoff = new Date(
      now - PRESENTED_TTL_MS,
    ).toISOString();

    const row = db
      .prepare(
        `SELECT COUNT(*) AS count
           FROM messages
          WHERE to_role = @role
            AND (
              (${EXPIRED_CLAIM_SQL})
              OR (${STALE_PRESENTED_SQL})
              OR (${EXPIRED_TAGGED_SQL})
            )`,
      )
      .get({
        role,
        now,
        presentedCutoff,
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
