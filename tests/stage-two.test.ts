import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import {
  dirname,
  join,
  resolve,
} from "node:path";
import test, {
  type TestContext,
} from "node:test";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import {
  BOUNCE_REASON,
  BOUNCE_SUBJECT,
  BridgeBus,
  BridgeConflictError,
  CLAIM_LEASE_MS,
  type EndpointRow,
  type MessageRow,
  type MigrationStep,
  MIGRATION_STEPS,
  PRESENTED_TTL_MS,
  type Role,
  SCHEMA_VERSION,
  type SendResult,
  TAG_TTL_MS,
  computeEnvelopeHash,
  deriveBounceMessageId,
  initializeBridgeDatabaseAtPath,
  migrateBridgeDatabaseAtPath,
  sha256,
} from "../src/db.js";

const PROJECT_ROOT = resolve(
  fileURLToPath(new URL("..", import.meta.url)),
);
const INIT_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-init.ts",
);
const T0 = Date.UTC(2026, 7, 30);
const ISO0 = new Date(T0).toISOString();

interface ProcessResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

interface V41Row {
  messageId: string;
  fromRole: Role;
  toRole: Role;
  toTag: string | null;
  fromTag: string | null;
  onTimeout: "bounce" | "fallback" | null;
  tagExpiresAt: number | null;
  subject: string;
  body: string;
  envelopeHash: string;
  status: MessageRow["status"];
  attemptId: string | null;
  consumer: string | null;
  leaseExpiresAt: number | null;
  attemptCount: number;
  presentedAt: string | null;
  ackedAt: string | null;
}

interface DeliveryShape {
  state: string;
  holder: string | null;
  attempt_id: string | null;
  attempt_count: number;
  lease_until: number | null;
  presented_at: string | null;
  confirmed_at: string | null;
}

const V41_SCHEMA_SQL = `
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE messages (
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
    CHECK (status IN ('stored','claimed','presented','acked','rejected','bounced')),
  attempt_id TEXT,
  consumer TEXT,
  lease_expires_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sent_at TEXT NOT NULL,
  presented_at TEXT,
  acked_at TEXT,
  CHECK (from_role <> to_role),
  CHECK (
    (to_tag IS NULL AND on_timeout IS NULL AND tag_expires_at IS NULL)
    OR
    (to_tag IS NOT NULL AND on_timeout IN ('bounce','fallback') AND tag_expires_at IS NOT NULL)
    OR
    (to_tag IS NOT NULL AND on_timeout IS NULL AND tag_expires_at IS NULL)
  )
);
CREATE INDEX idx_inbox ON messages (to_role, status, id);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT,
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
`;

const V32_SCHEMA_SQL = `
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
  root_id TEXT NOT NULL,
  from_role TEXT NOT NULL CHECK (from_role IN ('claude','codex')),
  to_role TEXT NOT NULL CHECK (to_role IN ('claude','codex')),
  subject TEXT NOT NULL,
  body TEXT NOT NULL,
  envelope_sha256 TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  sender_thread_id TEXT,
  status TEXT NOT NULL DEFAULT 'stored'
    CHECK (status IN ('stored','claimed','presented','acked','rejected')),
  attempt_id TEXT,
  consumer TEXT,
  lease_expires_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sent_at TEXT NOT NULL,
  presented_at TEXT,
  acked_at TEXT,
  CHECK (from_role <> to_role)
);
CREATE INDEX idx_inbox ON messages (to_role, status, id);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT,
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
`;

const V41_LEGAL_SHAPES = [
  {
    toTag: null,
    onTimeout: null,
    tagExpiresAt: null,
  },
  {
    toTag: "lane",
    onTimeout: "bounce",
    tagExpiresAt: T0 + TAG_TTL_MS,
  },
  {
    toTag: "lane",
    onTimeout: null,
    tagExpiresAt: null,
  },
] as const;

function withDb<T>(
  dbPath: string,
  work: (
    db: InstanceType<typeof Database>,
  ) => T,
): T {
  const db = new Database(dbPath, {
    fileMustExist: true,
  });

  try {
    return work(db);
  } finally {
    db.close();
  }
}

function makePath(
  t: TestContext,
  prefix: string,
): string {
  const directory = mkdtempSync(
    join(tmpdir(), prefix),
  );

  t.after(() => {
    rmSync(directory, {
      recursive: true,
      force: true,
    });
  });

  return join(directory, "bridge.db");
}

function makeDb(
  t: TestContext,
  prefix = "agent-bridge-stage-two-",
): string {
  const dbPath = makePath(t, prefix);
  initializeBridgeDatabaseAtPath(dbPath);
  return dbPath;
}

function makeProfile(
  t: TestContext,
  prefix: string,
): {
  userProfile: string;
  dbPath: string;
} {
  const userProfile = mkdtempSync(
    join(tmpdir(), prefix),
  );

  t.after(() => {
    rmSync(userProfile, {
      recursive: true,
      force: true,
    });
  });

  return {
    userProfile,
    dbPath: join(
      userProfile,
      ".claude",
      "data",
      "agent-bridge",
      "bridge.db",
    ),
  };
}

function writeFixtureDatabase(
  dbPath: string,
  version: string,
  schema: string,
): void {
  mkdirSync(dirname(dbPath), {
    recursive: true,
  });

  const db = new Database(dbPath);
  try {
    db.pragma("journal_mode = WAL");
    db.exec(schema);

    const insert = db.prepare(
      "INSERT INTO meta (k, v) VALUES (?, ?)",
    );
    insert.run("root_id", randomUUID());
    insert.run("schema_version", version);
    insert.run("created_at", ISO0);
  } finally {
    db.close();
  }
}

function writeV41Db(dbPath: string): void {
  writeFixtureDatabase(
    dbPath,
    "4.1",
    V41_SCHEMA_SQL,
  );
}

function writeV32Db(dbPath: string): void {
  writeFixtureDatabase(
    dbPath,
    "3.2",
    V32_SCHEMA_SQL,
  );
}

function makeV41Db(
  t: TestContext,
  prefix = "agent-bridge-v41-stage-two-",
): string {
  const dbPath = makePath(t, prefix);
  writeV41Db(dbPath);
  return dbPath;
}

function legacyEnvelopeHash(
  row: Pick<
    V41Row,
    | "fromRole"
    | "toRole"
    | "subject"
    | "body"
    | "toTag"
    | "onTimeout"
    | "fromTag"
  >,
): string {
  return sha256(
    JSON.stringify([
      row.fromRole,
      row.toRole,
      row.subject,
      row.body,
      row.toTag,
      row.onTimeout,
      row.fromTag,
    ]),
  );
}

function fixtureRow(
  messageId: string,
  status: MessageRow["status"],
  overrides: Partial<V41Row> = {},
): V41Row {
  const attempted = [
    "claimed",
    "presented",
    "acked",
    "rejected",
  ].includes(status);
  const hasLegacyLease = [
    "claimed",
    "presented",
    "acked",
    "rejected",
    "bounced",
  ].includes(status);
  const shown = [
    "presented",
    "acked",
  ].includes(status);
  const returnedToStored = status === "stored";
  const row: V41Row = {
    messageId,
    fromRole: "claude",
    toRole: "codex",
    toTag: null,
    fromTag: null,
    onTimeout: null,
    tagExpiresAt: null,
    subject: `subject-${messageId}`,
    body: `body-${messageId}`,
    envelopeHash: "",
    status,
    attemptId:
      attempted ? randomUUID() : null,
    consumer:
      attempted ? "codex:fixture" : null,
    leaseExpiresAt:
      hasLegacyLease
        ? T0 + CLAIM_LEASE_MS
        : null,
    attemptCount:
      attempted || returnedToStored
        ? 1
        : 0,
    presentedAt:
      shown || returnedToStored
        ? new Date(T0 + 1).toISOString()
        : null,
    ackedAt:
      status === "acked"
        ? new Date(T0 + 2).toISOString()
        : null,
    ...overrides,
  };

  row.envelopeHash =
    overrides.envelopeHash ??
    legacyEnvelopeHash(row);
  return row;
}

function migrationFixtureRows(): V41Row[] {
  return [
    fixtureRow("destination-a", "stored", {
      subject: "same payload",
      body: "same body",
    }),
    fixtureRow("destination-b", "stored", {
      ...V41_LEGAL_SHAPES[2],
      subject: "same payload",
      body: "same body",
    }),
    fixtureRow("status-claimed", "claimed", {
      ...V41_LEGAL_SHAPES[1],
      fromTag: "sender-claimed",
    }),
    fixtureRow(
      "status-presented",
      "presented",
    ),
    fixtureRow("status-acked", "acked", {
      fromTag: "sender-acked",
    }),
    fixtureRow("status-rejected", "rejected", {
      ...V41_LEGAL_SHAPES[1],
    }),
    fixtureRow("status-bounced", "bounced", {
      ...V41_LEGAL_SHAPES[1],
    }),
    fixtureRow("bounce-without-deadline", "stored", {
      ...V41_LEGAL_SHAPES[2],
      subject: BOUNCE_SUBJECT,
      body: "stored bounce fixture",
    }),
  ];
}

function seedV41Rows(
  dbPath: string,
  rows = migrationFixtureRows(),
): V41Row[] {
  withDb(dbPath, (db) => {
    const rootId = (
      db
        .prepare(
          "SELECT v FROM meta WHERE k = 'root_id'",
        )
        .get() as { v: string }
    ).v;
    const insert = db.prepare(
      `INSERT INTO messages (
         message_id, root_id, from_role, to_role, to_tag, from_tag,
         on_timeout, tag_expires_at, subject, body, envelope_sha256,
         body_sha256, sender_thread_id, status, attempt_id, consumer,
         lease_expires_at, attempt_count, sent_at, presented_at, acked_at
       ) VALUES (
         @messageId, @rootId, @fromRole, @toRole, @toTag, @fromTag,
         @onTimeout, @tagExpiresAt, @subject, @body, @envelopeHash,
         @bodyHash, NULL, @status, @attemptId, @consumer,
         @leaseExpiresAt, @attemptCount, @sentAt, @presentedAt, @ackedAt
       )`,
    );

    for (const row of rows) {
      insert.run({
        ...row,
        rootId,
        bodyHash: sha256(row.body),
        sentAt: ISO0,
      });
    }
  });

  return rows;
}

function insertV32Message(
  dbPath: string,
  messageId: string,
): void {
  withDb(dbPath, (db) => {
    const rootId = (
      db
        .prepare(
          "SELECT v FROM meta WHERE k = 'root_id'",
        )
        .get() as { v: string }
    ).v;
    const subject = "v32 subject";
    const body = "v32 body";

    db.prepare(
      `INSERT INTO messages (
         message_id, root_id, from_role, to_role, subject, body,
         envelope_sha256, body_sha256, status, sent_at
       ) VALUES (?, ?, 'claude', 'codex', ?, ?, ?, ?, 'stored', ?)`,
    ).run(
      messageId,
      rootId,
      subject,
      body,
      sha256(
        JSON.stringify([
          "claude",
          "codex",
          subject,
          body,
        ]),
      ),
      sha256(body),
      ISO0,
    );
  });
}

function insertCurrentMessage(
  db: InstanceType<typeof Database>,
  messageId: string,
  fromRole: Role,
): void {
  const toRole: Role =
    fromRole === "claude"
      ? "codex"
      : "claude";
  const subject = `fixture-${messageId}`;
  const body = "fixture body";

  db.prepare(
    `INSERT INTO messages (
       message_id, from_role, to_role, subject, body,
       envelope_sha256, envelope_version, body_sha256,
       status, sent_at
     ) VALUES (?, ?, ?, ?, ?, ?, 2, ?, 'stored', ?)`,
  ).run(
    messageId,
    fromRole,
    toRole,
    subject,
    body,
    computeEnvelopeHash(
      fromRole,
      subject,
      body,
    ),
    sha256(body),
    ISO0,
  );
}

async function runBridgeInitProcess(
  userProfile: string,
  args: readonly string[],
): Promise<ProcessResult> {
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      INIT_ENTRY,
      ...args,
    ],
    {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        USERPROFILE: userProfile,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stdout = "";
  let stderr = "";

  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on(
    "data",
    (chunk: string) => {
      stdout += chunk;
    },
  );
  child.stderr.on(
    "data",
    (chunk: string) => {
      stderr += chunk;
    },
  );

  const [code] = (await once(
    child,
    "close",
  )) as [
    number | null,
    NodeJS.Signals | null,
  ];

  return { code, stdout, stderr };
}

function tableRowCount(
  dbPath: string,
  table: string,
): number {
  return withDb(dbPath, (db) =>
    (
      db
        .prepare(
          `SELECT COUNT(*) AS count FROM ${table}`,
        )
        .get() as { count: number }
    ).count,
  );
}

function databaseSnapshot(
  dbPath: string,
): string {
  return withDb(dbPath, (db) =>
    JSON.stringify({
      messages: db
        .prepare(
          "SELECT * FROM messages ORDER BY id",
        )
        .all(),
      deliveries: db
        .prepare(
          "SELECT * FROM deliveries ORDER BY delivery_id",
        )
        .all(),
      events: db
        .prepare(
          "SELECT * FROM events ORDER BY seq",
        )
        .all(),
    }),
  );
}

function rowCounts(
  dbPath: string,
): {
  messages: number;
  deliveries: number;
  events: number;
} {
  return {
    messages: tableRowCount(
      dbPath,
      "messages",
    ),
    deliveries: tableRowCount(
      dbPath,
      "deliveries",
    ),
    events: tableRowCount(
      dbPath,
      "events",
    ),
  };
}

function readDelivery(
  dbPath: string,
  messageId: string,
): DeliveryShape {
  return withDb(dbPath, (db) =>
    db
      .prepare(
        `SELECT state, holder, attempt_id, attempt_count,
                lease_until, presented_at, confirmed_at
           FROM deliveries
          WHERE message_id = ?`,
      )
      .get(messageId) as DeliveryShape,
  );
}

function sendOutcome(
  result: SendResult,
): "inserted" | "idempotent" | "refused" {
  if ("kind" in result) {
    return result.kind;
  }

  return result.idempotent
    ? "idempotent"
    : "inserted";
}

function lastEventDetail(
  dbPath: string,
  messageId: string,
  event: string,
): string {
  return withDb(dbPath, (db) =>
    (
      db
        .prepare(
          `SELECT detail
             FROM events
            WHERE message_id = ?
              AND event = ?
            ORDER BY seq DESC
            LIMIT 1`,
        )
        .get(
          messageId,
          event,
        ) as { detail: string }
    ).detail,
  );
}

function tableSql(
  dbPath: string,
  table: "messages" | "deliveries",
): string {
  return withDb(dbPath, (db) =>
    (
      db
        .prepare(
          "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .get(table) as { sql: string }
    ).sql.replace(
      new RegExp(
        `^CREATE TABLE "${table}"`,
      ),
      `CREATE TABLE ${table}`,
    ),
  );
}

function expectedFilledDelivery(
  row: V41Row,
): DeliveryShape {
  const state = {
    stored: "pending",
    claimed: "leased",
    presented: "presented",
    acked: "confirmed",
    rejected: "rejected",
    bounced: "bounced",
  }[row.status];

  return {
    state,
    holder: [
      "claimed",
      "presented",
      "acked",
      "rejected",
      "bounced",
    ].includes(row.status)
      ? row.consumer
      : null,
    attempt_id: [
      "claimed",
      "presented",
      "acked",
      "rejected",
      "bounced",
    ].includes(row.status)
      ? row.attemptId
      : null,
    attempt_count: row.attemptCount,
    lease_until:
      row.status === "claimed"
        ? row.leaseExpiresAt
        : null,
    presented_at: [
      "presented",
      "acked",
      "bounced",
    ].includes(row.status)
      ? row.presentedAt
      : null,
    confirmed_at:
      row.status === "acked"
        ? row.ackedAt
        : null,
  };
}

test(
  "v38-1 send writes message+delivery+event; a forced failure after the message insert leaves all three at 0 rows",
  (t) => {
    const successful = makeDb(
      t,
      "agent-bridge-v38-1-ok-",
    );
    const bus = BridgeBus.open(successful);

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "stored atomically",
        body: "one delivery",
        messageId: randomUUID(),
        now: T0,
      });
    } finally {
      bus.close();
    }

    assert.deepEqual(
      rowCounts(successful),
      {
        messages: 1,
        deliveries: 1,
        events: 1,
      },
    );

    const failed = makeDb(
      t,
      "agent-bridge-v38-1-fail-",
    );
    withDb(failed, (db) => {
      db.exec(`
CREATE TRIGGER force_delivery_failure
BEFORE INSERT ON deliveries
BEGIN
  SELECT RAISE(ABORT, 'forced delivery failure');
END;
`);
    });

    const failingBus = BridgeBus.open(failed);
    try {
      assert.throws(
        () =>
          failingBus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "roll back",
            body: "all rows",
            messageId: randomUUID(),
            now: T0,
          }),
        /forced delivery failure/,
      );
    } finally {
      failingBus.close();
    }

    assert.equal(
      databaseSnapshot(failed),
      JSON.stringify({
        messages: [],
        deliveries: [],
        events: [],
      }),
    );
  },
);

test(
  "v38-2 each of the seven edges, driven one at a time, leaves deliveries in the v10 D-4 column state; attempt_count rises only on claim",
  async (t) => {
    const edges = [
      "claim",
      "lease expiry",
      "present",
      "presented requeue",
      "ack",
      "reject",
      "bounce",
    ] as const;

    for (const edge of edges) {
      await t.test(edge, (st) => {
        const dbPath = makeDb(
          st,
          `agent-bridge-v38-2-${edge.replaceAll(" ", "-")}-`,
        );
        const bus = BridgeBus.open(dbPath);
        const messageId = randomUUID();
        const consumer = `codex:${edge}`;
        let expected: DeliveryShape;

        try {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: edge,
            body: "body",
            messageId,
            now: T0,
            ...(edge === "bounce"
              ? {
                  toTag: "expired",
                  onTimeout: "bounce",
                }
              : {}),
          });

          if (edge === "bounce") {
            bus.recover(
              "codex",
              T0 + TAG_TTL_MS + 1,
            );
            expected = {
              state: "bounced",
              holder: null,
              attempt_id: null,
              attempt_count: 0,
              lease_until: null,
              presented_at: null,
              confirmed_at: null,
            };
          } else {
            if (edge === "reject") {
              withDb(dbPath, (db) => {
                db.prepare(
                  "UPDATE messages SET body_sha256 = 'poison' WHERE message_id = ?",
                ).run(messageId);
              });
            }

            const claimed = bus.claim(
              "codex",
              consumer,
              1,
              T0,
            );
            const attemptId =
              edge === "reject"
                ? bus.readMessage(messageId)!
                    .attempt_id!
                : claimed[0]!.attempt_id;

            expected = {
              state: "leased",
              holder: consumer,
              attempt_id: attemptId,
              attempt_count: 1,
              lease_until:
                T0 + CLAIM_LEASE_MS,
              presented_at: null,
              confirmed_at: null,
            };

            if (edge === "lease expiry") {
              bus.recover(
                "codex",
                T0 + CLAIM_LEASE_MS + 1,
              );
              expected = {
                ...expected,
                state: "pending",
                holder: null,
                attempt_id: null,
                lease_until: null,
              };
            }

            if (
              edge === "present" ||
              edge === "presented requeue" ||
              edge === "ack"
            ) {
              bus.markPresented(
                "codex",
                consumer,
                [
                  {
                    messageId,
                    attemptId,
                  },
                ],
                T0 + 1,
              );
              expected = {
                ...expected,
                state: "presented",
                lease_until: null,
                presented_at:
                  new Date(
                    T0 + 1,
                  ).toISOString(),
              };
            }

            if (edge === "presented requeue") {
              bus.recover(
                "codex",
                T0 +
                  1 +
                  PRESENTED_TTL_MS +
                  1,
              );
              expected = {
                ...expected,
                state: "pending",
                holder: null,
                attempt_id: null,
                presented_at: null,
              };
            }

            if (edge === "ack") {
              bus.ack(
                "codex",
                messageId,
                attemptId,
                T0 + 2,
                consumer,
              );
              expected = {
                ...expected,
                state: "confirmed",
                confirmed_at:
                  new Date(
                    T0 + 2,
                  ).toISOString(),
              };
            }

            if (edge === "reject") {
              expected = {
                ...expected,
                state: "rejected",
                lease_until: null,
              };
            }
          }

          assert.deepEqual(
            readDelivery(
              dbPath,
              messageId,
            ),
            expected,
          );
        } finally {
          bus.close();
        }
      });
    }
  },
);

test(
  "v38-3 envelope: changing each obligation default changes v2; changing each of the five destination/sender fields does not; false/undefined for expects_reply differ from 0; the three write paths use the one function",
  (t) => {
    const rawV2 = (
      inReplyTo: unknown,
      replyKind: unknown,
      expectsReply: unknown,
    ) =>
      sha256(
        JSON.stringify([
          2,
          "claude",
          "subject",
          "body",
          inReplyTo,
          replyKind,
          expectsReply,
        ]),
      );
    const canonical =
      computeEnvelopeHash(
        "claude",
        "subject",
        "body",
      );

    assert.equal(
      canonical,
      rawV2(null, null, 0),
    );
    for (const changed of [
      rawV2("parent", null, 0),
      rawV2(null, "answer", 0),
      rawV2(null, null, 1),
      rawV2(null, null, false),
      rawV2(null, null, undefined),
    ]) {
      assert.notEqual(changed, canonical);
    }

    const candidates = [
      { toRole: "codex" },
      { toTag: "lane" },
      { onTimeout: "fallback" },
      { fromTag: "sender" },
      { sourceEndpointId: randomUUID() },
    ].map((changed) => ({
      fromRole: "claude" as const,
      subject: "subject",
      body: "body",
      toRole: "claude" as Role,
      toTag: null as string | null,
      onTimeout: null as string | null,
      fromTag: null as string | null,
      sourceEndpointId: null as string | null,
      ...changed,
    }));

    for (const row of candidates) {
      assert.equal(
        computeEnvelopeHash(
          row.fromRole,
          row.subject,
          row.body,
        ),
        canonical,
      );
    }

    const source = readFileSync(
      fileURLToPath(
        new URL(
          "../src/db.ts",
          import.meta.url,
        ),
      ),
      "utf8",
    );
    const legacyHashFunction =
      source.match(
        /function computeLegacyEnvelopeHash\([\s\S]*?\n}\n/,
      )?.[0];
    const envelopeHashFunction =
      source.match(
        /export function computeEnvelopeHash\([\s\S]*?\n}\n/,
      )?.[0];

    assert.ok(legacyHashFunction);
    assert.ok(envelopeHashFunction);
    assert.equal(
      source.match(
        /JSON\.stringify\(\[\s*2,/g,
      )?.length,
      1,
    );
    assert.equal(
      envelopeHashFunction.match(
        /JSON\.stringify\(\[\s*2,/g,
      )?.length,
      1,
    );
    assert.equal(
      source.match(
        /\bsha256\(\s*JSON\.stringify\(/g,
      )?.length,
      2,
    );
    assert.equal(
      legacyHashFunction.match(
        /\bsha256\(\s*JSON\.stringify\(/g,
      )?.length,
      1,
    );
    assert.equal(
      envelopeHashFunction.match(
        /\bsha256\(\s*JSON\.stringify\(/g,
      )?.length,
      1,
    );

    const dbPath = makeDb(
      t,
      "agent-bridge-v38-3-live-",
    );
    const bus = BridgeBus.open(dbPath);
    const sentId = randomUUID();
    const bouncedId = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "send path",
        body: "send body",
        messageId: sentId,
        now: T0,
      });
      assert.equal(
        bus.readMessage(sentId)!
          .envelope_sha256,
        computeEnvelopeHash(
          "claude",
          "send path",
          "send body",
        ),
      );

      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "expires",
        body: "bounce body",
        messageId: bouncedId,
        toTag: "gone",
        fromTag: "home",
        onTimeout: "bounce",
        now: T0,
      });
      bus.recover(
        "codex",
        T0 + TAG_TTL_MS + 1,
      );

      const bounceMessageId =
        deriveBounceMessageId(bouncedId);
      const bounceBody =
        `${BOUNCE_REASON}; ` +
        `original_message_id=${bouncedId}`;
      assert.equal(
        bus.readMessage(bounceMessageId)!
          .envelope_sha256,
        computeEnvelopeHash(
          "codex",
          BOUNCE_SUBJECT,
          bounceBody,
        ),
      );
    } finally {
      bus.close();
    }

    const migrated = makeV41Db(
      t,
      "agent-bridge-v38-3-migration-",
    );
    const copied = fixtureRow(
      "migration-hash-path",
      "stored",
      {
        ...V41_LEGAL_SHAPES[2],
        subject: "migration path",
        body: "migration body",
      },
    );
    seedV41Rows(migrated, [copied]);
    migrateBridgeDatabaseAtPath(migrated);

    withDb(migrated, (db) => {
      const row = db
        .prepare(
          `SELECT envelope_sha256, envelope_version
             FROM messages
            WHERE message_id = ?`,
        )
        .get(copied.messageId) as {
        envelope_sha256: string;
        envelope_version: number;
      };

      assert.equal(
        row.envelope_sha256,
        computeEnvelopeHash(
          copied.fromRole,
          copied.subject,
          copied.body,
        ),
      );
      assert.equal(row.envelope_version, 2);
    });
  },
);

test(
  "v38-4 attribution table, five rows; the conflict detail contains sender_mismatch and no endpoint name",
  async (t) => {
    const rows = [
      {
        initial: "x",
        initialTag: "tag-a",
        retries: [
          {
            source: "x",
            tag: "tag-a",
            conflict: false,
          },
        ],
      },
      {
        initial: "x",
        initialTag: "tag-a",
        retries: [
          {
            source: "y",
            tag: "tag-a",
            conflict: true,
          },
        ],
      },
      {
        initial: "x",
        initialTag: "tag-a",
        retries: [
          {
            source: null,
            tag: "tag-a",
            conflict: true,
          },
        ],
      },
      {
        initial: null,
        initialTag: "tag-a",
        retries: [
          {
            source: null,
            tag: "tag-a",
            conflict: false,
          },
          {
            source: null,
            tag: "tag-b",
            conflict: true,
          },
        ],
      },
      {
        initial: null,
        initialTag: "tag-a",
        retries: [
          {
            source: "y",
            tag: "tag-a",
            conflict: true,
          },
        ],
      },
    ] as const;

    for (const [index, row] of rows.entries()) {
      await t.test(`row ${index + 1}`, (st) => {
        const dbPath = makeDb(
          st,
          `agent-bridge-v38-4-${index}-`,
        );
        const bus = BridgeBus.open(dbPath);
        const sourceX = bus.addEndpoint(
          "claude",
          "source-alpha",
        );
        const sourceY = bus.addEndpoint(
          "claude",
          "source-beta",
        );
        const endpoint = (
          name: "x" | "y" | null,
        ): EndpointRow | null =>
          name === "x"
            ? sourceX
            : name === "y"
              ? sourceY
              : null;
        const messageId = randomUUID();

        try {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "attribution",
            body: "same",
            messageId,
            fromTag: row.initialTag,
            sourceEndpoint:
              endpoint(row.initial),
            now: T0,
          });

          for (const retry of row.retries) {
            const resend = () =>
              bus.send({
                fromRole: "claude",
                toRole: "codex",
                subject: "attribution",
                body: "same",
                messageId,
                fromTag: retry.tag,
                sourceEndpoint:
                  endpoint(retry.source),
                now: T0 + 1,
              });

            if (retry.conflict) {
              assert.throws(
                resend,
                BridgeConflictError,
              );
              const detail =
                lastEventDetail(
                  dbPath,
                  messageId,
                  "send_conflict",
                );
              assert.deepEqual(
                JSON.parse(detail),
                { sender_mismatch: true },
              );
              assert.doesNotMatch(
                detail,
                /source-alpha|source-beta/,
              );
            } else {
              assert.equal(
                sendOutcome(resend()),
                "idempotent",
              );
            }
          }
        } finally {
          bus.close();
        }
      });
    }
  },
);

test(
  "v38-6 on a fresh database with an empty registry, an untagged send, a tagged send and a broadcast: true send all succeed and each produces exactly one delivery with endpoint_id NULL",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      for (const input of [
        {},
        { toTag: "lane" },
        { broadcast: true },
      ]) {
        assert.equal(
          sendOutcome(
            bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject: JSON.stringify(input),
              body: "body",
              messageId: randomUUID(),
              now: T0,
              ...input,
            }),
          ),
          "inserted",
        );
      }
    } finally {
      bus.close();
    }

    withDb(dbPath, (db) => {
      assert.equal(
        (
          db
            .prepare(
              "SELECT COUNT(*) AS count FROM endpoints",
            )
            .get() as { count: number }
        ).count,
        0,
      );
      assert.deepEqual(
        db
          .prepare(
            "SELECT endpoint_id FROM deliveries ORDER BY delivery_id",
          )
          .all(),
        [
          { endpoint_id: null },
          { endpoint_id: null },
          { endpoint_id: null },
        ],
      );
    });
  },
);

test(
  "v38-7 to_endpoint: a registered name fills endpoint_id; an unknown, a retired and a wrong-role name are refused with three distinct messages and no message, delivery or event row is written",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      const registered =
        bus.addEndpoint(
          "codex",
          "registered",
        );
      const retired =
        bus.addEndpoint(
          "codex",
          "retired",
        );
      bus.addEndpoint(
        "claude",
        "wrong-role",
      );

      withDb(dbPath, (db) => {
        db.prepare(
          "UPDATE endpoints SET retired_at = ? WHERE endpoint_id = ?",
        ).run(ISO0, retired.endpoint_id);
      });

      const stored = bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "registered",
        body: "body",
        messageId: randomUUID(),
        toEndpoint: "registered",
        now: T0,
      });
      assert.equal(
        sendOutcome(stored),
        "inserted",
      );

      withDb(dbPath, (db) => {
        assert.equal(
          (
            db
              .prepare(
                "SELECT endpoint_id FROM deliveries",
              )
              .get() as {
              endpoint_id: string;
            }
          ).endpoint_id,
          registered.endpoint_id,
        );
      });

      const refusals = [
        [
          "missing",
          /no endpoint named "missing" is registered/,
        ],
        [
          "retired",
          /endpoint codex\/retired was retired at /,
        ],
        [
          "wrong-role",
          /endpoint "wrong-role" is registered for claude, not codex/,
        ],
      ] as const;

      for (const [
        name,
        message,
      ] of refusals) {
        const before = rowCounts(dbPath);
        assert.throws(
          () =>
            bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject: name,
              body: "body",
              messageId: randomUUID(),
              toEndpoint: name,
              now: T0,
            }),
          message,
        );
        assert.deepEqual(
          rowCounts(dbPath),
          before,
        );
      }
    } finally {
      bus.close();
    }
  },
);

test(
  "v38-8 the five rows of v19 D-2, each with its idempotent/refused outcome, send_refused event, and no new rows on refusal",
  async (t) => {
    const rows = [
      {
        initial: { toTag: "lane-a" },
        retries: [
          {
            input: { toTag: "lane-a" },
            outcome: "idempotent",
          },
        ],
      },
      {
        initial: { toTag: "lane-a" },
        retries: [
          {
            input: { toTag: "lane-b" },
            outcome: "refused",
          },
        ],
      },
      {
        initial: { toTag: "lane-a" },
        retries: [
          {
            input: {
              toEndpoint: "receiver-a",
            },
            outcome: "refused",
          },
        ],
      },
      {
        initial: {
          toEndpoint: "receiver-a",
        },
        retries: [
          {
            input: {
              toEndpoint: "receiver-a",
            },
            outcome: "idempotent",
          },
        ],
      },
      {
        initial: {
          toEndpoint: "receiver-a",
        },
        retries: [
          {
            input: {},
            outcome: "refused",
          },
          {
            input: {
              toEndpoint: "receiver-b",
            },
            outcome: "refused",
          },
        ],
      },
    ] as const;

    for (const [index, row] of rows.entries()) {
      await t.test(`row ${index + 1}`, (st) => {
        const dbPath = makeDb(
          st,
          `agent-bridge-v38-8-${index}-`,
        );
        const bus = BridgeBus.open(dbPath);
        bus.addEndpoint(
          "codex",
          "receiver-a",
        );
        bus.addEndpoint(
          "codex",
          "receiver-b",
        );
        const messageId = randomUUID();

        try {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "same",
            body: "same",
            messageId,
            now: T0,
            ...row.initial,
          });

          for (const retry of row.retries) {
            const before =
              rowCounts(dbPath);
            const result = bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject: "same",
              body: "same",
              messageId,
              now: T0 + 1,
              ...retry.input,
            });

            assert.equal(
              sendOutcome(result),
              retry.outcome,
            );

            const after = rowCounts(dbPath);
            assert.equal(
              after.messages,
              before.messages,
            );
            assert.equal(
              after.deliveries,
              before.deliveries,
            );
            assert.equal(
              after.events,
              before.events +
                (retry.outcome === "refused"
                  ? 1
                  : 0),
            );

            if (
              retry.outcome === "refused"
            ) {
              assert.deepEqual(
                JSON.parse(
                  lastEventDetail(
                    dbPath,
                    messageId,
                    "send_refused",
                  ),
                ),
                {
                  reason:
                    "second_delivery_before_stage4",
                },
              );
            }
          }
        } finally {
          bus.close();
        }
      });
    }
  },
);

test(
  "v38-9 --migrate from a 4.1 fixture normalises every row and a 3.2 fixture also lands at 4.9",
  async (t) => {
    const profile = makeProfile(
      t,
      "agent-bridge-v38-9-v41-",
    );
    writeV41Db(profile.dbPath);
    const seeds = seedV41Rows(
      profile.dbPath,
    );
    const beforeHashes = new Map(
      seeds.map((row) => [
        row.messageId,
        row.envelopeHash,
      ]),
    );

    const migrated =
      await runBridgeInitProcess(
        profile.userProfile,
        ["--migrate"],
      );

    assert.equal(
      migrated.code,
      0,
      migrated.stderr,
    );

    withDb(profile.dbPath, (db) => {
      const rows = db
        .prepare(
          `SELECT message_id, to_tag, legacy_to_tag,
                  envelope_sha256, envelope_version
             FROM messages
            ORDER BY id`,
        )
        .all() as Array<{
        message_id: string;
        to_tag: string | null;
        legacy_to_tag: string | null;
        envelope_sha256: string;
        envelope_version: number;
      }>;

      assert.equal(rows.length, seeds.length);
      for (const row of rows) {
        assert.equal(row.envelope_version, 2);
        assert.notEqual(
          row.envelope_sha256,
          beforeHashes.get(row.message_id),
        );
        assert.equal(
          row.legacy_to_tag,
          row.to_tag,
        );
      }

      const destinationHashes = rows
        .filter((row) =>
          row.message_id.startsWith(
            "destination-",
          ),
        )
        .map(
          (row) => row.envelope_sha256,
        );
      assert.equal(
        destinationHashes[0],
        destinationHashes[1],
      );

      const deliveries = db
        .prepare(
          `SELECT message_id, endpoint_id, state,
                  holder, attempt_id, attempt_count,
                  lease_until, presented_at, confirmed_at
             FROM deliveries
            ORDER BY delivery_id`,
        )
        .all() as Array<
        DeliveryShape & {
          message_id: string;
          endpoint_id: string | null;
        }
      >;

      assert.equal(
        deliveries.length,
        seeds.length,
      );
      for (const delivery of deliveries) {
        const {
          message_id: messageId,
          endpoint_id: endpointId,
          ...shape
        } = delivery;
        assert.equal(endpointId, null);
        const seed = seeds.find(
          (row) => row.messageId === messageId,
        )!;
        assert.deepEqual(
          shape,
          expectedFilledDelivery(seed),
        );
      }

      const triggers = (
        db
          .prepare(
            `SELECT name
               FROM sqlite_master
              WHERE type = 'trigger'
              ORDER BY name`,
          )
          .all() as Array<{ name: string }>
      ).map((row) => row.name);

      for (const name of [
        "deliveries_identity_immutable",
        "deliveries_role_differs",
        "messages_identity_immutable",
      ]) {
        assert.ok(
          triggers.includes(name),
          name,
        );
      }

      const senderEndpoint =
        randomUUID();
      db.prepare(
        `INSERT INTO endpoints (
           endpoint_id, role, name, created_at
         ) VALUES (?, 'claude', 'sender-role', ?)`,
      ).run(senderEndpoint, ISO0);

      const triggerMessage =
        randomUUID();
      insertCurrentMessage(
        db,
        triggerMessage,
        "claude",
      );

      assert.throws(
        () =>
          db
            .prepare(
              `INSERT INTO deliveries (
                 message_id, endpoint_id, state
               ) VALUES (?, ?, 'pending')`,
            )
            .run(
              triggerMessage,
              senderEndpoint,
            ),
        /delivery to the sender role/,
      );
    });

    const v32 = makeProfile(
      t,
      "agent-bridge-v38-9-v32-",
    );
    writeV32Db(v32.dbPath);
    insertV32Message(
      v32.dbPath,
      randomUUID(),
    );

    const migratedV32 =
      await runBridgeInitProcess(
        v32.userProfile,
        ["--migrate"],
      );

    assert.equal(
      migratedV32.code,
      0,
      migratedV32.stderr,
    );
    withDb(v32.dbPath, (db) => {
      assert.equal(
        (
          db
            .prepare(
              "SELECT v FROM meta WHERE k = 'schema_version'",
            )
            .get() as { v: string }
        ).v,
        SCHEMA_VERSION,
      );
    });
    assert.equal(
      tableRowCount(
        v32.dbPath,
        "deliveries",
      ),
      1,
    );
  },
);

test(
  "v38-11 the eight identity columns of messages refuse UPDATE; status still updates",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    const sourceA = bus.addEndpoint(
      "claude",
      "source-a",
    );
    const sourceB = bus.addEndpoint(
      "claude",
      "source-b",
    );
    const messageId = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "identity",
        body: "body",
        messageId,
        toTag: "lane",
        sourceEndpoint: sourceA,
        now: T0,
      });
    } finally {
      bus.close();
    }

    withDb(dbPath, (db) => {
      const updates: Array<
        [string, unknown]
      > = [
        ["message_id", randomUUID()],
        ["from_role", "codex"],
        [
          "source_endpoint_id",
          sourceB.endpoint_id,
        ],
        ["legacy_to_tag", "other"],
        ["subject", "changed"],
        ["body", "changed"],
        [
          "envelope_sha256",
          sha256("changed"),
        ],
        ["envelope_version", 3],
      ];

      for (const [column, value] of updates) {
        assert.throws(
          () =>
            db
              .prepare(
                `UPDATE messages SET ${column} = ? WHERE message_id = ?`,
              )
              .run(value, messageId),
          /message identity is immutable/,
          column,
        );
      }

      assert.equal(
        db
          .prepare(
            "UPDATE messages SET status = 'bounced' WHERE message_id = ?",
          )
          .run(messageId).changes,
        1,
      );
      assert.equal(
        (
          db
            .prepare(
              "SELECT status FROM messages WHERE message_id = ?",
            )
            .get(messageId) as {
            status: string;
          }
        ).status,
        "bounced",
      );
    });
  },
);

test(
  "v38-12 fresh init and migrated database have identical sqlite_master.sql bodies for messages and deliveries",
  (t) => {
    const fresh = makeDb(
      t,
      "agent-bridge-v38-12-fresh-",
    );
    const migrated = makeV41Db(
      t,
      "agent-bridge-v38-12-migrated-",
    );
    seedV41Rows(
      migrated,
      [
        fixtureRow(
          "schema-copy",
          "stored",
        ),
      ],
    );
    migrateBridgeDatabaseAtPath(migrated);

    for (const table of [
      "messages",
      "deliveries",
    ] as const) {
      assert.equal(
        tableSql(migrated, table),
        tableSql(fresh, table),
      );
    }
  },
);

test.skip(
  "v38-13 public/main failures are recorded in NOTES",
  () => {},
);

test(
  "v38-19 one delivery per message: mixed NULL and endpoint destinations fail until the one-per-message index is dropped",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    const endpoint = bus.addEndpoint(
      "codex",
      "receiver-a",
    );
    const other = bus.addEndpoint(
      "codex",
      "receiver-b",
    );
    const nullFirst = randomUUID();
    const endpointFirst = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "null first",
        body: "body",
        messageId: nullFirst,
        now: T0,
      });
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "endpoint first",
        body: "body",
        messageId: endpointFirst,
        toEndpoint: endpoint.name,
        now: T0,
      });
    } finally {
      bus.close();
    }

    withDb(dbPath, (db) => {
      const insert = db.prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state
         ) VALUES (?, ?, 'pending')`,
      );

      assert.throws(
        () =>
          insert.run(
            nullFirst,
            endpoint.endpoint_id,
          ),
        /UNIQUE constraint failed: deliveries\.message_id/,
      );
      assert.throws(
        () =>
          insert.run(
            endpointFirst,
            null,
          ),
        /UNIQUE constraint failed: deliveries\.message_id/,
      );
      assert.throws(
        () =>
          insert.run(
            endpointFirst,
            other.endpoint_id,
          ),
        /UNIQUE constraint failed: deliveries\.message_id/,
      );

      db.exec(
        "DROP INDEX deliveries_one_per_message",
      );

      insert.run(
        nullFirst,
        endpoint.endpoint_id,
      );
      insert.run(endpointFirst, null);
      insert.run(
        endpointFirst,
        other.endpoint_id,
      );

      assert.equal(
        (
          db
            .prepare(
              "SELECT COUNT(*) AS count FROM deliveries WHERE message_id = ?",
            )
            .get(nullFirst) as {
            count: number;
          }
        ).count,
        2,
      );
      assert.equal(
        (
          db
            .prepare(
              "SELECT COUNT(*) AS count FROM deliveries WHERE message_id = ?",
            )
            .get(endpointFirst) as {
            count: number;
          }
        ).count,
        3,
      );
    });
  },
);

test(
  "v38-20 the delivery identity trigger permits NULL to E once, rejects E to E-prime, and rejects message_id changes while endpoint_id is NULL",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    const endpoint = bus.addEndpoint(
      "codex",
      "receiver-a",
    );
    const other = bus.addEndpoint(
      "codex",
      "receiver-b",
    );
    const assignable = randomUUID();
    const withoutDelivery = randomUUID();
    const movable = randomUUID();

    try {
      for (const messageId of [
        assignable,
        withoutDelivery,
        movable,
      ]) {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: messageId,
          body: "body",
          messageId,
          now: T0,
        });
      }
    } finally {
      bus.close();
    }

    withDb(dbPath, (db) => {
      db.prepare(
        "DELETE FROM deliveries WHERE message_id = ?",
      ).run(withoutDelivery);

      assert.equal(
        db
          .prepare(
            "UPDATE deliveries SET endpoint_id = ? WHERE message_id = ?",
          )
          .run(
            endpoint.endpoint_id,
            assignable,
          ).changes,
        1,
      );

      assert.throws(
        () =>
          db
            .prepare(
              "UPDATE deliveries SET endpoint_id = ? WHERE message_id = ?",
            )
            .run(
              other.endpoint_id,
              assignable,
            ),
        /delivery message\/endpoint are immutable/,
      );

      assert.equal(
        (
          db
            .prepare(
              "SELECT endpoint_id FROM deliveries WHERE message_id = ?",
            )
            .get(movable) as {
            endpoint_id: string | null;
          }
        ).endpoint_id,
        null,
      );

      assert.throws(
        () =>
          db
            .prepare(
              "UPDATE deliveries SET message_id = ? WHERE message_id = ?",
            )
            .run(
              withoutDelivery,
              movable,
            ),
        /delivery message\/endpoint are immutable/,
      );
    });
  },
);

test(
  "v38-21 after fill every message has one NULL delivery in its normalised state; an empty 4.1 database also migrates",
  (t) => {
    const dbPath = makeV41Db(t);
    const seeds = seedV41Rows(dbPath);
    migrateBridgeDatabaseAtPath(dbPath);

    assert.equal(
      tableRowCount(dbPath, "deliveries"),
      seeds.length,
    );

    for (const seed of seeds) {
      const delivery = readDelivery(
        dbPath,
        seed.messageId,
      );
      assert.deepEqual(
        delivery,
        expectedFilledDelivery(seed),
      );

      withDb(dbPath, (db) => {
        assert.equal(
          (
            db
              .prepare(
                "SELECT endpoint_id FROM deliveries WHERE message_id = ?",
              )
              .get(seed.messageId) as {
              endpoint_id: string | null;
            }
          ).endpoint_id,
          null,
        );
      });
    }

    const empty = makeV41Db(
      t,
      "agent-bridge-v38-21-empty-",
    );
    const metadata =
      migrateBridgeDatabaseAtPath(empty);

    assert.equal(
      metadata.schemaVersion,
      SCHEMA_VERSION,
    );
    assert.equal(
      tableRowCount(empty, "deliveries"),
      0,
    );
  },
);

test(
  "v38-23 a 4.6 database whose deliveries holds a row refuses the 4.6 to 4.7 step and the version does not move",
  (t) => {
    const dbPath = makeV41Db(t);
    const seed = fixtureRow(
      "v46-existing-delivery",
      "stored",
    );
    seedV41Rows(dbPath, [seed]);

    const throughV46: MigrationStep[] =
      MIGRATION_STEPS.filter((step) =>
        [
          "4.1",
          "4.2",
          "4.3",
          "4.4",
          "4.5",
        ].includes(step.from),
      ).map((step) =>
        step.from === "4.5"
          ? {
              ...step,
              to: SCHEMA_VERSION,
            }
          : step,
      );

    migrateBridgeDatabaseAtPath(
      dbPath,
      {},
      throughV46,
    );

    withDb(dbPath, (db) => {
      db.prepare(
        "UPDATE meta SET v = '4.6' WHERE k = 'schema_version'",
      ).run();

      const endpointId = randomUUID();
      db.prepare(
        `INSERT INTO endpoints (
           endpoint_id, role, name, created_at
         ) VALUES (?, 'codex', 'v46-receiver', ?)`,
      ).run(endpointId, ISO0);
      db.prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state
         ) VALUES (?, ?, 'pending')`,
      ).run(seed.messageId, endpointId);
    });

    assert.throws(
      () =>
        migrateBridgeDatabaseAtPath(
          dbPath,
        ),
      /deliveries must be empty before stage two/,
    );

    withDb(dbPath, (db) => {
      assert.equal(
        (
          db
            .prepare(
              "SELECT v FROM meta WHERE k = 'schema_version'",
            )
            .get() as { v: string }
        ).v,
        "4.6",
      );
      assert.equal(
        tableRowCount(
          dbPath,
          "deliveries",
        ),
        1,
      );
    });
  },
);

test(
  "v38-24 destination retries use legacy_to_tag after fallback and keep untagged broadcast retries idempotent",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    const tagged = randomUUID();
    const untagged = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "fallback",
        body: "body",
        messageId: tagged,
        toTag: "original",
        onTimeout: "fallback",
        now: T0,
      });

      assert.equal(
        sendOutcome(
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "fallback",
            body: "body",
            messageId: tagged,
            toTag: "different",
            onTimeout: "fallback",
            now: T0 + 1,
          }),
        ),
        "refused",
      );
      assert.equal(
        sendOutcome(
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "fallback",
            body: "body",
            messageId: tagged,
            toTag: "original",
            onTimeout: "fallback",
            now: T0 + 1,
          }),
        ),
        "idempotent",
      );

      bus.recover(
        "codex",
        T0 + TAG_TTL_MS + 1,
      );
      assert.equal(
        bus.readMessage(tagged)!.to_tag,
        null,
      );

      assert.equal(
        sendOutcome(
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "fallback",
            body: "body",
            messageId: tagged,
            toTag: "original",
            onTimeout: "fallback",
            now: T0 + TAG_TTL_MS + 2,
          }),
        ),
        "idempotent",
      );

      for (const destination of [
        { toTag: "different" },
        {},
      ]) {
        assert.equal(
          sendOutcome(
            bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject: "fallback",
              body: "body",
              messageId: tagged,
              now:
                T0 +
                TAG_TTL_MS +
                2,
              ...destination,
            }),
          ),
          "refused",
        );
      }

      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "broadcast",
        body: "body",
        messageId: untagged,
        now: T0,
      });
      assert.equal(
        sendOutcome(
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "broadcast",
            body: "body",
            messageId: untagged,
            broadcast: true,
            now: T0 + 1,
          }),
        ),
        "idempotent",
      );
    } finally {
      bus.close();
    }
  },
);

test(
  "v38-25 send writes legacy_to_tag; migration copies to_tag and leaves demoted rows NULL; the identity trigger rejects later updates",
  (t) => {
    const fresh = makeDb(
      t,
      "agent-bridge-v38-25-fresh-",
    );
    const bus = BridgeBus.open(fresh);
    const sent = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "legacy tag",
        body: "body",
        messageId: sent,
        toTag: "live-lane",
        now: T0,
      });
      assert.equal(
        bus.readMessage(sent)!
          .legacy_to_tag,
        "live-lane",
      );
    } finally {
      bus.close();
    }

    const migrated = makeV41Db(
      t,
      "agent-bridge-v38-25-migrated-",
    );
    const tagged = fixtureRow(
      "tagged-before-migration",
      "stored",
      {
        ...V41_LEGAL_SHAPES[2],
      },
    );
    const demoted = fixtureRow(
      "demoted-before-migration",
      "stored",
    );
    seedV41Rows(
      migrated,
      [tagged, demoted],
    );

    withDb(migrated, (db) => {
      db.prepare(
        `INSERT INTO events (
           message_id, attempt_id, event, at, detail
         ) VALUES (?, NULL, 'tag_fallback', ?, NULL)`,
      ).run(demoted.messageId, ISO0);
    });

    migrateBridgeDatabaseAtPath(migrated);

    withDb(migrated, (db) => {
      const rows = db
        .prepare(
          `SELECT message_id, legacy_to_tag
             FROM messages
            WHERE message_id IN (?, ?)
            ORDER BY message_id`,
        )
        .all(
          tagged.messageId,
          demoted.messageId,
        ) as Array<{
        message_id: string;
        legacy_to_tag: string | null;
      }>;
      const byId = new Map(
        rows.map((row) => [
          row.message_id,
          row.legacy_to_tag,
        ]),
      );

      assert.equal(
        byId.get(tagged.messageId),
        tagged.toTag,
      );
      assert.equal(
        byId.get(demoted.messageId),
        null,
      );

      assert.throws(
        () =>
          db
            .prepare(
              "UPDATE messages SET legacy_to_tag = 'changed' WHERE message_id = ?",
            )
            .run(tagged.messageId),
        /message identity is immutable/,
      );
    });
  },
);

test(
  "v38-26 bounce reuse rejects a different destination without writes and is idempotent for the matching destination",
  (t) => {
    const conflicting = makeDb(
      t,
      "agent-bridge-v38-26-conflict-",
    );
    const conflictingBus =
      BridgeBus.open(conflicting);
    const conflictingOriginal = randomUUID();
    const conflictingBounce =
      deriveBounceMessageId(
        conflictingOriginal,
      );
    const conflictingBody =
      `${BOUNCE_REASON}; ` +
      `original_message_id=${conflictingOriginal}`;

    try {
      conflictingBus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "expires",
        body: "body",
        messageId: conflictingOriginal,
        toTag: "gone",
        fromTag: "home",
        onTimeout: "bounce",
        now: T0,
      });
      conflictingBus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: BOUNCE_SUBJECT,
        body: conflictingBody,
        messageId: conflictingBounce,
        toTag: "wrong",
        now: T0,
      });
      const before =
        databaseSnapshot(conflicting);

      assert.throws(
        () =>
          conflictingBus.recover(
            "codex",
            T0 + TAG_TTL_MS + 1,
          ),
        (error: unknown) => {
          assert.ok(
            error instanceof BridgeConflictError,
          );
          assert.equal(
            error.message,
            `bounce message_id ${conflictingBounce} already exists with a different envelope`,
          );
          return true;
        },
      );
      assert.equal(
        conflictingBus.readMessage(
          conflictingOriginal,
        )!.status,
        "stored",
      );
      assert.equal(
        databaseSnapshot(conflicting),
        before,
      );
    } finally {
      conflictingBus.close();
    }

    const matching = makeDb(
      t,
      "agent-bridge-v38-26-matching-",
    );
    const matchingBus = BridgeBus.open(matching);
    const matchingOriginal = randomUUID();
    const matchingBounce =
      deriveBounceMessageId(matchingOriginal);
    const matchingBody =
      `${BOUNCE_REASON}; ` +
      `original_message_id=${matchingOriginal}`;

    try {
      matchingBus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "expires",
        body: "body",
        messageId: matchingOriginal,
        toTag: "gone",
        fromTag: "home",
        onTimeout: "bounce",
        now: T0,
      });
      matchingBus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: BOUNCE_SUBJECT,
        body: matchingBody,
        messageId: matchingBounce,
        toTag: "home",
        now: T0,
      });
      const existingBounce =
        matchingBus.readMessage(matchingBounce);

      assert.equal(
        matchingBus.recover(
          "codex",
          T0 + TAG_TTL_MS + 1,
        ).bounced,
        1,
      );
      assert.equal(
        matchingBus.readMessage(
          matchingOriginal,
        )!.status,
        "bounced",
      );
      assert.deepEqual(
        matchingBus.readMessage(matchingBounce),
        existingBounce,
      );

      const afterFirstRecovery =
        databaseSnapshot(matching);
      assert.equal(
        matchingBus.recover(
          "codex",
          T0 + TAG_TTL_MS + 2,
        ).bounced,
        0,
      );
      assert.equal(
        databaseSnapshot(matching),
        afterFirstRecovery,
      );
    } finally {
      matchingBus.close();
    }
  },
);