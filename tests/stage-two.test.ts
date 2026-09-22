import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
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
  BOUNCE_SUBJECT,
  BridgeBus,
  BridgeConflictError,
  CLAIM_LEASE_MS,
  type EndpointMapping,
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
  envelopeHashSeam,
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

const MAPPING: EndpointMapping = {
  endpoints: [
    { role: "claude", name: "claude-main" },
    { role: "codex", name: "codex-main" },
    { role: "claude", name: "sender-claimed" },
    { role: "claude", name: "sender-acked" },
  ],
  tags: [
    { role: "claude", tag: null, endpoint: "claude-main" },
    { role: "codex", tag: null, endpoint: "codex-main" },
    { role: "codex", tag: "lane", endpoint: "codex-main" },
    {
      role: "claude",
      tag: "sender-claimed",
      endpoint: "sender-claimed",
    },
    {
      role: "claude",
      tag: "sender-acked",
      endpoint: "sender-acked",
    },
  ],
};

process.env.AGENT_BRIDGE_TEST_PROCESS_SCAN = "quiet";

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
  sourceEndpointId: string,
): void {
  const subject = `fixture-${messageId}`;
  const body = "fixture body";

  db.prepare(
    `INSERT INTO messages (
       message_id, from_role, source_endpoint_id, subject, body,
       envelope_sha256, envelope_version, body_sha256,
       sent_at
     ) VALUES (?, ?, ?, ?, ?, ?, 2, ?, ?)`,
  ).run(
    messageId,
    fromRole,
    sourceEndpointId,
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
): "inserted" | "idempotent" {
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
             FROM message_events
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

function triggerSql(
  dbPath: string,
  trigger: string,
): string {
  return withDb(dbPath, (db) =>
    (
      db
        .prepare(
          "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?",
        )
        .get(trigger) as { sql: string }
    ).sql,
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
    const source = bus.addEndpoint(
      "claude",
      "claude-main",
    );
    bus.addEndpoint("codex", "codex-main");

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "stored atomically",
        body: "one delivery",
        messageId: randomUUID(),
        sourceEndpoint: source,
        toEndpoints: ["codex-main"],
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
    const failingSource = failingBus.addEndpoint(
      "claude",
      "claude-main",
    );
    failingBus.addEndpoint("codex", "codex-main");
    try {
      assert.throws(
        () =>
          failingBus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "roll back",
            body: "all rows",
            messageId: randomUUID(),
            sourceEndpoint: failingSource,
            toEndpoints: ["codex-main"],
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
  "v38-2 each of the six edges, driven one at a time, leaves deliveries in the v10 D-4 column state; attempt_count rises only on claim",
  async (t) => {
    const edges = [
      "claim",
      "lease expiry",
      "present",
      "presented requeue",
      "ack",
      "reject",
    ] as const;

    for (const edge of edges) {
      await t.test(edge, (st) => {
        const dbPath = makeDb(
          st,
          `agent-bridge-v38-2-${edge.replaceAll(" ", "-")}-`,
        );
        const bus = BridgeBus.open(dbPath);
        const source = bus.addEndpoint(
          "claude",
          "claude-main",
        );
        const destination = bus.addEndpoint(
          "codex",
          "codex-main",
        );
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
            sourceEndpoint: source,
            toEndpoints: ["codex-main"],
            now: T0,
          });


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
              null,
              destination,
            );
            const attemptId =
              edge === "reject"
                ? (
                    withDb(dbPath, (db) =>
                      db
                        .prepare(
                          "SELECT attempt_id FROM deliveries WHERE message_id = ?",
                        )
                        .get(messageId) as {
                        attempt_id: string;
                      },
                    )
                  ).attempt_id
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
                destination,
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
                destination,
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
  "v38-3 envelope: changing each obligation default changes v2; changing each of the five destination/sender fields does not; false/undefined for expects_reply differ from 0; send and migration use the one function",
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

    type EnvelopeHashArguments = Parameters<
      typeof computeEnvelopeHash
    >;

    const exerciseWritePaths = (
      prefix: string,
    ) => {
      const sentId = randomUUID();
      const sendArgs: EnvelopeHashArguments = [
        "claude",
        "send path",
        "send body",
      ];
      const dbPath = makeDb(
        t,
        `agent-bridge-v38-3-${prefix}-live-`,
      );
      const bus = BridgeBus.open(dbPath);
      const source = bus.addEndpoint(
        "claude",
        "claude-main",
      );
      bus.addEndpoint("codex", "codex-main");

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: sendArgs[1],
          body: sendArgs[2],
          messageId: sentId,
          sourceEndpoint: source,
          toEndpoints: ["codex-main"],
          now: T0,
        });
        const sendHash =
          bus.readMessage(sentId)!
            .envelope_sha256;


        const migrated = makeV41Db(
          t,
          `agent-bridge-v38-3-${prefix}-migration-`,
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
        const migrationArgs:
          EnvelopeHashArguments = [
            copied.fromRole,
            copied.subject,
            copied.body,
          ];
        seedV41Rows(migrated, [copied]);
        migrateBridgeDatabaseAtPath(migrated, {
          mapping: MAPPING,
        });

        const migrationHash = withDb(
          migrated,
          (db) => {
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
              row.envelope_version,
              2,
            );
            return row.envelope_sha256;
          },
        );

        return {
          send: {
            args: sendArgs,
            hash: sendHash,
          },
          migration: {
            args: migrationArgs,
            hash: migrationHash,
          },
        };
      } finally {
        bus.close();
      }
    };

    const writePaths = [
      "send",
      "migration",
    ] as const;
    const originalCompute =
      envelopeHashSeam.compute;
    assert.equal(
      originalCompute,
      computeEnvelopeHash,
    );

    const realWrites =
      exerciseWritePaths("real");
    for (const path of writePaths) {
      assert.equal(
        realWrites[path].hash,
        computeEnvelopeHash(
          ...realWrites[path].args
        ),
      );
    }

    const stubCompute:
      typeof envelopeHashSeam.compute =
        (...args) =>
          sha256(
            "stub:" +
              JSON.stringify([...args]),
          );

    try {
      envelopeHashSeam.compute = stubCompute;
      const stubWrites =
        exerciseWritePaths("stub");

      for (const path of writePaths) {
        assert.equal(
          stubWrites[path].hash,
          stubCompute(
            ...stubWrites[path].args
          ),
        );
        assert.notEqual(
          stubWrites[path].hash,
          realWrites[path].hash,
        );
      }
    } finally {
      envelopeHashSeam.compute =
        originalCompute;
    }
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
            source: "x",
            tag: "tag-b",
            conflict: false,
          },
        ],
      },
      {
        initial: "x",
        initialTag: "tag-a",
        retries: [
          {
            source: "x",
            tag: "tag-a",
            conflict: false,
          },
          {
            source: "y",
            tag: "tag-b",
            conflict: true,
          },
        ],
      },

      {
        initial: "y",
        initialTag: "tag-a",
        retries: [
          {
            source: "x",
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
        bus.addEndpoint("codex", "codex-main");
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
            toEndpoints: ["codex-main"],
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
                toEndpoints: ["codex-main"],
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
  "v38-9 --migrate from a 4.1 fixture normalises every row and a 3.2 fixture also lands at the current version",
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

    const mappingPath = join(
      profile.userProfile,
      "mapping.json",
    );
    writeFileSync(
      mappingPath,
      JSON.stringify(MAPPING),
      "utf8",
    );
    const migrated =
      await runBridgeInitProcess(
        profile.userProfile,
        ["--migrate", "--mapping", mappingPath],
      );

    assert.equal(
      migrated.code,
      0,
      migrated.stderr,
    );

    withDb(profile.dbPath, (db) => {
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

      const rows = db
        .prepare(
          `SELECT message_id, legacy_to_tag,
                  envelope_sha256, envelope_version
             FROM messages
            ORDER BY id`,
        )
        .all() as Array<{
        message_id: string;
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
          seeds.find(
            (item) =>
              item.messageId === row.message_id,
          )!.toTag,
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
          `SELECT d.message_id AS message_id,
                  ep.name AS endpoint_name,
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
            ORDER BY d.delivery_id`,
        )
        .all() as Array<
        DeliveryShape & {
          message_id: string;
          endpoint_name: string;
        }
      >;

      assert.equal(
        deliveries.length,
        seeds.length,
      );
      for (const delivery of deliveries) {
        const {
          message_id: messageId,
          endpoint_name: endpointName,
          ...shape
        } = delivery;
        assert.equal(endpointName, "codex-main");
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
        "deliveries_role_differs_on_assign",
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
        senderEndpoint,
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
    const v32MappingPath = join(
      v32.userProfile,
      "mapping.json",
    );
    writeFileSync(
      v32MappingPath,
      JSON.stringify(MAPPING),
      "utf8",
    );

    const migratedV32 =
      await runBridgeInitProcess(
        v32.userProfile,
        ["--migrate", "--mapping", v32MappingPath],
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
  "v38-11 the nine identity columns of messages refuse UPDATE; attempt_count still updates",
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
    bus.addEndpoint("codex", "codex-main");
    const messageId = randomUUID();

    try {
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "identity",
        body: "body",
        messageId,
        sourceEndpoint: sourceA,
        toEndpoints: ["codex-main"],
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
        ["legacy_from_tag", "other"],
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
            "UPDATE messages SET attempt_count = 4 WHERE message_id = ?",
          )
          .run(messageId).changes,
        1,
      );
      assert.equal(
        (
          db
            .prepare(
              "SELECT attempt_count FROM messages WHERE message_id = ?",
            )
            .get(messageId) as {
            attempt_count: number;
          }
        ).attempt_count,
        4,
      );
    });
  },
);

test(
  "v38-12 fresh init and migrated database have identical sqlite_master.sql bodies for messages, deliveries, and deliveries_role_differs_on_assign",
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
    migrateBridgeDatabaseAtPath(migrated, {
      mapping: MAPPING,
    });

    for (const table of [
      "messages",
      "deliveries",
    ] as const) {
      assert.equal(
        tableSql(migrated, table),
        tableSql(fresh, table),
      );
    }

    assert.equal(
      triggerSql(
        migrated,
        "deliveries_role_differs_on_assign",
      ),
      triggerSql(
        fresh,
        "deliveries_role_differs_on_assign",
      ),
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
  "v38-25 migration copies to_tag into legacy_to_tag and leaves demoted rows NULL; the identity trigger rejects later updates",
  (t) => {


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

    migrateBridgeDatabaseAtPath(migrated, {
      mapping: MAPPING,
    });

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
  "v38-28 an exact retry stays idempotent after endpoint retirement; a new send to the retired endpoint is refused",
  (t) => {
    const dbPath = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    const source = bus.addEndpoint(
      "claude",
      "claude-main",
    );
    const endpoint = bus.addEndpoint(
      "codex",
      "receiver",
    );
    const messageId = randomUUID();
    const send = (id: string) =>
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "retirement",
        body: "body",
        messageId: id,
        sourceEndpoint: source,
        toEndpoints: ["receiver"],
        now: T0,
      });

    try {
      assert.equal(
        sendOutcome(send(messageId)),
        "inserted",
      );

      withDb(dbPath, (db) => {
        db.prepare(
          "UPDATE endpoints SET retired_at = ? WHERE endpoint_id = ?",
        ).run(ISO0, endpoint.endpoint_id);
      });

      const beforeRetry =
        databaseSnapshot(dbPath);
      assert.equal(
        sendOutcome(send(messageId)),
        "idempotent",
      );
      assert.equal(
        databaseSnapshot(dbPath),
        beforeRetry,
      );

      const beforeFresh =
        databaseSnapshot(dbPath);
      assert.throws(
        () => send(randomUUID()),
        /endpoint codex\/receiver was retired at /,
      );
      assert.equal(
        databaseSnapshot(dbPath),
        beforeFresh,
      );
    } finally {
      bus.close();
    }
  },
);

test(
  "v40-1 open and migrate reject a non-UUID meta.root_id and quote its value",
  (t) => {
    const dbPath = makeDb(
      t,
      "agent-bridge-v40-1-",
    );
    const invalidRootId = "not a uuid";

    withDb(dbPath, (db) => {
      assert.equal(
        db
          .prepare(
            "UPDATE meta SET v = ? WHERE k = 'root_id'",
          )
          .run(invalidRootId).changes,
        1,
      );
    });

    const operations = [
      () => {
        const bus = BridgeBus.open(dbPath);
        bus.close();
      },
      () => {
        migrateBridgeDatabaseAtPath(dbPath);
      },
    ];

    for (const operation of operations) {
      assert.throws(
        operation,
        (error: unknown) => {
          assert.ok(error instanceof Error);
          assert.equal(
            error.name,
            "BridgeDatabaseError",
          );
          assert.equal(
            error.message,
            `meta.root_id is not a UUIDv4: "not a uuid"`,
          );
          assert.doesNotMatch(
            error.message,
            /UUIDv4: not a uuid/,
          );
          return true;
        },
      );
    }
  },
);

test(
  "v40-2 open and migrate keep an empty meta.root_id as missing",
  (t) => {
    const opened = makeDb(
      t,
      "agent-bridge-v40-2-open-",
    );
    const migrated = makeV41Db(
      t,
      "agent-bridge-v40-2-migrate-",
    );

    for (const dbPath of [
      opened,
      migrated,
    ]) {
      withDb(dbPath, (db) => {
        assert.equal(
          db
            .prepare(
              "UPDATE meta SET v = ? WHERE k = 'root_id'",
            )
            .run("").changes,
          1,
        );
      });
    }

    const operations = [
      () => {
        const bus = BridgeBus.open(opened);
        bus.close();
      },
      () => {
        migrateBridgeDatabaseAtPath(migrated);
      },
    ];

    for (const operation of operations) {
      assert.throws(
        operation,
        (error: unknown) => {
          assert.ok(error instanceof Error);
          assert.equal(
            error.name,
            "BridgeDatabaseError",
          );
          assert.equal(
            error.message,
            "meta.root_id is missing",
          );
          assert.doesNotMatch(
            error.message,
            /not a UUIDv4/,
          );
          return true;
        },
      );
    }
  },
);

test(
  "v40-3 valid UUIDv4 values still open and migrate; init keeps its invalid-root message",
  (t) => {
    const rootId = randomUUID();
    const opened = makePath(
      t,
      "agent-bridge-v40-3-open-",
    );
    const initialized =
      initializeBridgeDatabaseAtPath(
        opened,
        rootId,
      );

    assert.equal(initialized.rootId, rootId);

    const bus = BridgeBus.open(opened);
    try {
      assert.equal(
        bus.metadata.rootId,
        rootId,
      );
      assert.equal(
        bus.metadata.schemaVersion,
        SCHEMA_VERSION,
      );
    } finally {
      bus.close();
    }

    const migrated = makeV41Db(
      t,
      "agent-bridge-v40-3-migrate-",
    );
    withDb(migrated, (db) => {
      assert.equal(
        db
          .prepare(
            "UPDATE meta SET v = ? WHERE k = 'root_id'",
          )
          .run(rootId).changes,
        1,
      );
    });

    const migratedMetadata =
      migrateBridgeDatabaseAtPath(migrated, {
          mapping: MAPPING,
      });
    assert.equal(
      migratedMetadata.rootId,
      rootId,
    );
    assert.equal(
      migratedMetadata.schemaVersion,
      SCHEMA_VERSION,
    );

    assert.throws(
      () =>
        initializeBridgeDatabaseAtPath(
          makePath(
            t,
            "agent-bridge-v40-3-invalid-",
          ),
          "not a uuid",
        ),
      (error: unknown) => {
        assert.ok(error instanceof Error);
        assert.equal(
          error.name,
          "BridgeDatabaseError",
        );
        assert.equal(
          error.message,
          "root_id must be a UUIDv4 string",
        );
        return true;
      },
    );
  },
);

test(
  "v40-4 a BLOB meta.root_id holding the bytes of a valid UUID is refused, not stringified through",
  (t) => {
    const dbPath = makeDb(
      t,
      "agent-bridge-v40-4-",
    );
    const db = new Database(dbPath);
    try {
      db.prepare(
        "UPDATE meta SET v = CAST(? AS BLOB) WHERE k = 'root_id'",
      ).run(randomUUID());
    } finally {
      db.close();
    }
    assert.throws(
      () => BridgeBus.open(dbPath),
      /meta\.root_id is not a UUIDv4: <object>/,
    );
  },
);
