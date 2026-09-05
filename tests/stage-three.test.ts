import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import {
  dirname,
  join,
} from "node:path";
import test, {
  type TestContext,
} from "node:test";
import Database from "better-sqlite3";
import {
  BridgeBus,
  BridgeConflictError,
  CLAIM_LEASE_MS,
  PRESENTED_TTL_MS,
  SCHEMA_VERSION,
  TAG_TTL_MS,
  computeEnvelopeHash,
  deriveBounceMessageId,
  initializeBridgeDatabaseAtPath,
  lostQuerySql,
  migrateBridgeDatabaseAtPath,
  sha256,
  type Role,
} from "../src/db.js";
import {
  countPendingClaudeMessages,
  createHookOutput,
} from "../src/hook-notify.js";
import { formatBacklog } from "../src/bridge-sweep.js";
import { quoteForOneField } from "../src/one-line.js";

const T0 = Date.UTC(2026, 8, 5);
const ISO0 = new Date(T0).toISOString();

interface EventFixture {
  seq: number;
  attemptId: string | null;
  event: string;
  at: string;
  detail: string | null;
}

interface HistoryRow {
  seq: number;
  attempt_id: string | null;
  event: string;
  at: string;
  detail: string | null;
}

interface CurrentMessageOptions {
  fromRole?: Role;
  toRole?: Role;
  status?: "stored" | "bounced";
  state?: "pending" | "bounced";
  events?: readonly EventFixture[];
}

interface ExpectedWriterEvent {
  messageId: string;
  event: string;
  detailIncludes?: string;
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

function makeCurrentDb(
  t: TestContext,
  prefix = "agent-bridge-v41-current-",
): string {
  const dbPath = makePath(t, prefix);
  initializeBridgeDatabaseAtPath(dbPath);
  return dbPath;
}

function writeLegacyDatabase(
  dbPath: string,
  version: "3.2" | "4.1",
): void {
  mkdirSync(dirname(dbPath), {
    recursive: true,
  });
  const db = new Database(dbPath);

  try {
    db.exec(
      version === "4.1"
        ? V41_SCHEMA_SQL
        : V32_SCHEMA_SQL,
    );
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

function makeLegacyDb(
  t: TestContext,
  version: "3.2" | "4.1",
): string {
  const dbPath = makePath(
    t,
    `agent-bridge-v41-${version.replace(".", "")}-`,
  );
  writeLegacyDatabase(dbPath, version);
  return dbPath;
}

function seedLegacyMessage(
  dbPath: string,
  version: "3.2" | "4.1",
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
    const subject = `subject-${messageId}`;
    const body = `body-${messageId}`;
    const bodyHash = sha256(body);

    if (version === "4.1") {
      db.prepare(
        `INSERT INTO messages (
           message_id, root_id, from_role, to_role, to_tag, from_tag,
           on_timeout, tag_expires_at, subject, body, envelope_sha256,
           body_sha256, sender_thread_id, status, attempt_id, consumer,
           lease_expires_at, attempt_count, sent_at, presented_at, acked_at
         ) VALUES (
           ?, ?, 'claude', 'codex', NULL, NULL,
           NULL, NULL, ?, ?, ?,
           ?, NULL, 'stored', NULL, NULL,
           NULL, 0, ?, NULL, NULL
         )`,
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
            null,
            null,
            null,
          ]),
        ),
        bodyHash,
        ISO0,
      );
      return;
    }

    db.prepare(
      `INSERT INTO messages (
         message_id, root_id, from_role, to_role, subject, body,
         envelope_sha256, body_sha256, sender_thread_id, status,
         attempt_id, consumer, lease_expires_at, attempt_count,
         sent_at, presented_at, acked_at
       ) VALUES (
         ?, ?, 'claude', 'codex', ?, ?,
         ?, ?, NULL, 'stored',
         NULL, NULL, NULL, 0,
         ?, NULL, NULL
       )`,
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
      bodyHash,
      ISO0,
    );
  });
}

function insertLegacyEvents(
  dbPath: string,
  messageId: string,
  events: readonly EventFixture[],
): void {
  withDb(dbPath, (db) => {
    const insert = db.prepare(
      `INSERT INTO events (
         seq, message_id, attempt_id, event, at, detail
       ) VALUES (
         @seq, @messageId, @attemptId, @event, @at, @detail
       )`,
    );

    for (const event of events) {
      insert.run({
        ...event,
        messageId,
      });
    }
  });
}

function insertCurrentMessage(
  db: InstanceType<typeof Database>,
  messageId: string,
  options: CurrentMessageOptions = {},
): number {
  const fromRole = options.fromRole ?? "claude";
  const toRole = options.toRole ?? "codex";
  const status = options.status ?? "stored";
  const state = options.state ?? "pending";
  const subject = `subject-${messageId}`;
  const body = `body-${messageId}`;

  db.prepare(
    `INSERT INTO messages (
       message_id, from_role, to_role, subject, body,
       envelope_sha256, envelope_version, body_sha256,
       status, sent_at
     ) VALUES (?, ?, ?, ?, ?, ?, 2, ?, ?, ?)`,
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
    status,
    ISO0,
  );

  const delivery = db
    .prepare(
      `INSERT INTO deliveries (
         message_id, endpoint_id, state
       ) VALUES (?, NULL, ?)`,
    )
    .run(messageId, state);
  const deliveryId = Number(
    delivery.lastInsertRowid,
  );

  const insertEvent = db.prepare(
    `INSERT INTO events (
       seq, delivery_id, attempt_id, event, at, detail
     ) VALUES (
       @seq, @deliveryId, @attemptId, @event, @at, @detail
     )`,
  );

  for (const event of options.events ?? []) {
    insertEvent.run({
      ...event,
      deliveryId,
    });
  }

  return deliveryId;
}

function downgradeToV49(
  dbPath: string,
): void {
  withDb(dbPath, (db) => {
    const downgrade = db.transaction(() => {
      db.exec(`
CREATE TABLE events_v49 (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT,
  attempt_id TEXT,
  event TEXT NOT NULL,
  at TEXT NOT NULL,
  detail TEXT
);
INSERT INTO events_v49 (
  seq, message_id, attempt_id, event, at, detail
)
SELECT e.seq, d.message_id, e.attempt_id, e.event, e.at, e.detail
  FROM events e
  JOIN deliveries d USING (delivery_id)
 ORDER BY e.seq;
DROP VIEW message_events;
DROP TABLE events;
ALTER TABLE events_v49 RENAME TO events;
`);
      db.prepare(
        "UPDATE meta SET v = '4.9' WHERE k = 'schema_version'",
      ).run();
    });

    downgrade.immediate();
  });
}

function makeV49Db(
  t: TestContext,
  seed?: (
    db: InstanceType<typeof Database>,
  ) => void,
): string {
  const dbPath = makeCurrentDb(
    t,
    "agent-bridge-v41-v49-",
  );

  if (seed) {
    withDb(dbPath, seed);
  }

  downgradeToV49(dbPath);
  return dbPath;
}

function historyRows(
  dbPath: string,
  messageId: string,
  source: "events" | "message_events",
): HistoryRow[] {
  return withDb(
    dbPath,
    (db) =>
      db
        .prepare(
          `SELECT seq, attempt_id, event, at, detail
             FROM ${source}
            WHERE message_id = ?
            ORDER BY seq`,
        )
        .all(messageId) as HistoryRow[],
  );
}

function tableColumns(
  dbPath: string,
  name: string,
): Array<{
  name: string;
  notnull: number;
}> {
  return withDb(
    dbPath,
    (db) =>
      db
        .prepare(`PRAGMA table_info(${name})`)
        .all() as Array<{
        name: string;
        notnull: number;
      }>,
  );
}

function schemaSql(
  dbPath: string,
  type: "table" | "view",
  name: string,
): string {
  return withDb(dbPath, (db) => {
    const row = db
      .prepare(
        "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
      )
      .get(type, name) as
      | { sql: string }
      | undefined;

    assert.ok(row, `${type} ${name} is missing`);
    return type === "table"
      ? row.sql.replace(
          new RegExp(
            `^CREATE TABLE "${name}"`,
          ),
          `CREATE TABLE ${name}`,
        )
      : row.sql;
  });
}

function readMeta(
  dbPath: string,
  key: string,
): string {
  return withDb(
    dbPath,
    (db) =>
      (
        db
          .prepare(
            "SELECT v FROM meta WHERE k = ?",
          )
          .get(key) as { v: string }
      ).v,
  );
}

function sendDefault(
  bus: BridgeBus,
  messageId: string,
  overrides: {
    subject?: string;
    body?: string;
    fromTag?: string;
    toTag?: string;
    onTimeout?: "bounce" | "fallback";
    now?: number;
  } = {},
) {
  return bus.send({
    fromRole: "claude",
    toRole: "codex",
    subject:
      overrides.subject ??
      `subject-${messageId}`,
    body:
      overrides.body ??
      `body-${messageId}`,
    messageId,
    fromTag: overrides.fromTag,
    toTag: overrides.toTag,
    onTimeout: overrides.onTimeout,
    now: overrides.now ?? T0,
  });
}

function assertWriterEvent(
  dbPath: string,
  expected: ExpectedWriterEvent,
): void {
  withDb(dbPath, (db) => {
    const delivery = db
      .prepare(
        `SELECT delivery_id
           FROM deliveries
          WHERE message_id = ?`,
      )
      .get(expected.messageId) as
      | { delivery_id: number }
      | undefined;
    assert.ok(delivery);

    const rows = db
      .prepare(
        `SELECT message_id, delivery_id, detail
           FROM message_events
          WHERE message_id = ?
            AND event = ?
          ORDER BY seq`,
      )
      .all(
        expected.messageId,
        expected.event,
      ) as Array<{
      message_id: string;
      delivery_id: number;
      detail: string | null;
    }>;

    const row =
      expected.detailIncludes === undefined
        ? rows.at(-1)
        : rows.find((candidate) =>
            candidate.detail?.includes(
              expected.detailIncludes!,
            ),
          );

    assert.ok(
      row,
      `${expected.event} was not written for ${expected.messageId}`,
    );
    assert.equal(
      row.message_id,
      expected.messageId,
    );
    assert.equal(
      row.delivery_id,
      delivery.delivery_id,
    );
  });
}

function runWriterScenario(
  t: TestContext,
  exercise: (
    bus: BridgeBus,
    dbPath: string,
  ) => readonly ExpectedWriterEvent[],
): void {
  const dbPath = makeCurrentDb(
    t,
    "agent-bridge-v41-writer-",
  );
  const bus = BridgeBus.open(dbPath);
  const expected = (() => {
    try {
      return exercise(bus, dbPath);
    } finally {
      bus.close();
    }
  })();

  for (const event of expected) {
    assertWriterEvent(dbPath, event);
  }
}

function eventFixtureRows(): EventFixture[] {
  return [
    {
      seq: 1,
      attemptId: null,
      event: "sent",
      at: ISO0,
      detail: null,
    },
    {
      seq: 2,
      attemptId: randomUUID(),
      event: "claimed",
      at: new Date(T0 + 1).toISOString(),
      detail: '{"consumer":"fixture"}',
    },
    {
      seq: 3,
      attemptId: null,
      event: "lease_expired",
      at: new Date(T0 + 2).toISOString(),
      detail: "fixture recovery",
    },
  ];
}

function sweepSummary(
  dbPath: string,
  now: number,
): string {
  return withDb(dbPath, (db) => {
    const backlog = (role: Role) => {
      const row = db
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
    };

    return (
      `agent-bridge sweep db=${quoteForOneField(
        dbPath,
      )} ` +
      `claude=lease:0,requeued:0,bounced:0,fallback:0,${formatBacklog(
        backlog("claude"),
        now,
      )} ` +
      `codex=lease:0,requeued:0,bounced:0,fallback:0,${formatBacklog(
        backlog("codex"),
        now,
      )}`
    );
  });
}

test(
  "v41-1 bridge_status history preserves event rows across 4.9 to 4.10",
  (t) => {
    const messageId = randomUUID();
    const expected = eventFixtureRows();
    const dbPath = makeV49Db(t, (db) => {
      insertCurrentMessage(db, messageId, {
        events: expected,
      });
    });

    const before = historyRows(
      dbPath,
      messageId,
      "events",
    );
    migrateBridgeDatabaseAtPath(dbPath);

    const bus = BridgeBus.open(dbPath);
    try {
      assert.deepEqual(
        bus.status(messageId).events.map(
          ({
            seq,
            attempt_id,
            event,
            at,
            detail,
          }) => ({
            seq,
            attempt_id,
            event,
            at,
            detail,
          }),
        ),
        before,
      );
    } finally {
      bus.close();
    }
  },
);

test(
  "v41-2 events is delivery-keyed and every writer attaches its event to the acted-on message",
  (t) => {
    const shapeDb = makeCurrentDb(t);
    const columns = tableColumns(
      shapeDb,
      "events",
    );
    assert.deepEqual(
      columns.map((column) => column.name),
      [
        "seq",
        "delivery_id",
        "attempt_id",
        "event",
        "at",
        "detail",
      ],
    );
    assert.equal(
      columns.find(
        (column) =>
          column.name === "delivery_id",
      )?.notnull,
      1,
    );
    assert.equal(
      columns.some(
        (column) =>
          column.name === "message_id",
      ),
      false,
    );
    assert.deepEqual(
      withDb(
        shapeDb,
        (db) =>
          db
            .prepare(
              "PRAGMA index_list(events)",
            )
            .all(),
      ),
      [],
    );
    assert.deepEqual(
      tableColumns(
        shapeDb,
        "message_events",
      ).map((column) => column.name),
      [
        "message_id",
        "endpoint_id",
        "seq",
        "delivery_id",
        "attempt_id",
        "event",
        "at",
        "detail",
      ],
    );
    assert.match(
      schemaSql(
        shapeDb,
        "view",
        "message_events",
      ),
      /SELECT d\.message_id, d\.endpoint_id, e\.\*/,
    );

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      return [{ messageId, event: "sent" }];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId, {
        fromTag: "sender-a",
      });
      assert.throws(
        () =>
          sendDefault(bus, messageId, {
            fromTag: "sender-b",
          }),
        BridgeConflictError,
      );
      return [
        {
          messageId,
          event: "send_conflict",
          detailIncludes:
            '"sender_mismatch":true',
        },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId, {
        fromTag: "sender",
      });
      assert.throws(
        () =>
          sendDefault(bus, messageId, {
            fromTag: "sender",
            body: "different body",
          }),
        BridgeConflictError,
      );
      return [
        {
          messageId,
          event: "send_conflict",
          detailIncludes:
            '"attempted_envelope_sha256"',
        },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      const refused = sendDefault(
        bus,
        messageId,
        { toTag: "other-lane" },
      );
      assert.ok("kind" in refused);
      assert.equal(refused.kind, "refused");
      return [
        {
          messageId,
          event: "send_refused",
          detailIncludes:
            "second_delivery_before_stage4",
        },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      assert.equal(
        bus.claim(
          "codex",
          "codex:v41-claim",
          1,
          T0 + 1,
        ).length,
        1,
      );
      return [
        { messageId, event: "claimed" },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      bus.claim(
        "codex",
        "codex:v41-lease",
        1,
        T0 + 1,
      );
      bus.recover(
        "codex",
        T0 + CLAIM_LEASE_MS + 2,
      );
      return [
        {
          messageId,
          event: "lease_expired",
        },
      ];
    });

    runWriterScenario(t, (bus, dbPath) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      withDb(dbPath, (db) => {
        db.prepare(
          `UPDATE messages
              SET body_sha256 = ?
            WHERE message_id = ?`,
        ).run("invalid", messageId);
      });
      bus.claim(
        "codex",
        "codex:v41-reject",
        1,
        T0 + 1,
      );
      return [
        { messageId, event: "rejected" },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      bus.fetch(
        "codex",
        "codex:v41-present",
        {
          messageId,
          now: T0 + 1,
        },
      );
      return [
        { messageId, event: "presented" },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId);
      bus.fetch(
        "codex",
        "codex:v41-requeue",
        {
          messageId,
          now: T0 + 1,
        },
      );
      bus.recover(
        "codex",
        T0 + PRESENTED_TTL_MS + 2,
      );
      return [
        { messageId, event: "requeued" },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      const consumer = "codex:v41-ack";
      sendDefault(bus, messageId);
      const fetched = bus.fetch(
        "codex",
        consumer,
        {
          messageId,
          now: T0 + 1,
        },
      );
      const attemptId =
        fetched.messages[0]?.attempt_id;
      assert.ok(attemptId);
      bus.ack(
        "codex",
        messageId,
        attemptId,
        T0 + 2,
        consumer,
      );
      return [
        { messageId, event: "acked" },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId, {
        toTag: "expired-fallback",
        onTimeout: "fallback",
      });
      bus.recover(
        "codex",
        T0 + TAG_TTL_MS + 1,
      );
      return [
        {
          messageId,
          event: "tag_fallback",
        },
      ];
    });

    runWriterScenario(t, (bus) => {
      const messageId = randomUUID();
      sendDefault(bus, messageId, {
        fromTag: "sender-lane",
        toTag: "expired-bounce",
        onTimeout: "bounce",
      });
      bus.recover(
        "codex",
        T0 + TAG_TTL_MS + 1,
      );
      return [
        { messageId, event: "bounced" },
        {
          messageId:
            deriveBounceMessageId(messageId),
          event: "sent",
        },
      ];
    });
  },
);

test(
  "v41-3 4.1 and 3.2 events migrate intact and an orphaned 4.9 event stops migration",
  (t) => {
    for (const version of [
      "4.1",
      "3.2",
    ] as const) {
      const dbPath = makeLegacyDb(t, version);
      const messageId = randomUUID();
      const events = eventFixtureRows();
      seedLegacyMessage(
        dbPath,
        version,
        messageId,
      );
      insertLegacyEvents(
        dbPath,
        messageId,
        events,
      );
      const before = historyRows(
        dbPath,
        messageId,
        "events",
      );

      migrateBridgeDatabaseAtPath(dbPath);

      withDb(dbPath, (db) => {
        const delivery = db
          .prepare(
            `SELECT delivery_id
               FROM deliveries
              WHERE message_id = ?`,
          )
          .get(messageId) as {
          delivery_id: number;
        };
        const after = db
          .prepare(
            `SELECT seq, delivery_id, attempt_id,
                    event, at, detail
               FROM message_events
              WHERE message_id = ?
              ORDER BY seq`,
          )
          .all(messageId) as Array<
          HistoryRow & {
            delivery_id: number;
          }
        >;

        assert.deepEqual(
          after.map(
            ({
              seq,
              attempt_id,
              event,
              at,
              detail,
            }) => ({
              seq,
              attempt_id,
              event,
              at,
              detail,
            }),
          ),
          before,
        );
        assert.equal(after.length, events.length);
        assert.equal(
          after.every(
            (event) =>
              event.delivery_id ===
              delivery.delivery_id,
          ),
          true,
        );
      });
      assert.equal(
        readMeta(dbPath, "schema_version"),
        SCHEMA_VERSION,
      );
    }

    const orphanedId = randomUUID();
    const orphaned = makeV49Db(t, (db) => {
      insertCurrentMessage(
        db,
        orphanedId,
        {
          events: [
            {
              seq: 1,
              attemptId: null,
              event: "sent",
              at: ISO0,
              detail: null,
            },
          ],
        },
      );
    });

    withDb(orphaned, (db) => {
      db.prepare(
        "DELETE FROM deliveries WHERE message_id = ?",
      ).run(orphanedId);
    });

    assert.throws(
      () =>
        migrateBridgeDatabaseAtPath(
          orphaned,
        ),
      /migration row-count mismatch: source=1 copied=0/,
    );
    assert.equal(
      readMeta(orphaned, "schema_version"),
      "4.9",
    );
    assert.deepEqual(
      tableColumns(
        orphaned,
        "events",
      ).map((column) => column.name),
      [
        "seq",
        "message_id",
        "attempt_id",
        "event",
        "at",
        "detail",
      ],
    );
  },
);

test(
  "v41-4 gapped event sequences preserve a lostQuerySql cursor across migration",
  (t) => {
    const messageId = randomUUID();
    const dbPath = makeV49Db(t, (db) => {
      insertCurrentMessage(db, messageId, {
        status: "bounced",
        state: "bounced",
        events: [3, 5, 9, 12].map(
          (seq) => ({
            seq,
            attemptId: null,
            event: "bounced",
            at: new Date(
              T0 + seq,
            ).toISOString(),
            detail: null,
          }),
        ),
      });
    });

    const before = withDb(
      dbPath,
      (db) =>
        db
          .prepare(
            `SELECT e.seq
               FROM messages m
               JOIN events e
                 ON e.message_id = m.message_id
                AND e.event = 'bounced'
              WHERE m.to_role = @role
                AND m.status = 'bounced'
                AND e.seq > @since
              ORDER BY e.seq
              LIMIT @limit`,
          )
          .all({
            role: "codex",
            since: 0,
            limit: 2,
          }) as Array<{ seq: number }>,
    );
    assert.deepEqual(
      before.map((row) => row.seq),
      [3, 5],
    );

    migrateBridgeDatabaseAtPath(dbPath);
    const bus = BridgeBus.open(dbPath);

    try {
      assert.deepEqual(
        bus
          .undelivered("codex", 5, 10)
          .lost.map((row) => row.seq),
        [9, 12],
      );
    } finally {
      bus.close();
    }
  },
);

test(
  "v41-5 fresh and migrated events and message_events sqlite_master bodies match",
  (t) => {
    const fresh = makeCurrentDb(
      t,
      "agent-bridge-v41-fresh-",
    );
    const migrated = makeV49Db(t);

    migrateBridgeDatabaseAtPath(migrated);

    assert.equal(
      schemaSql(migrated, "table", "events"),
      schemaSql(fresh, "table", "events"),
    );
    assert.equal(
      schemaSql(
        migrated,
        "view",
        "message_events",
      ),
      schemaSql(
        fresh,
        "view",
        "message_events",
      ),
    );
  },
);

test(
  "v41-6 v25-1 still pins the lostQuerySql page and count to the events primary key",
  (t) => {
    const dbPath = makeCurrentDb(t);
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      for (const [shape, text] of Object.entries(
        lostQuerySql(),
      )) {
        assert.match(
          text,
          /JOIN message_events e/,
        );
        const details = (
          db
            .prepare(
              `EXPLAIN QUERY PLAN ${text}`,
            )
            .all({
              role: "claude",
              since: 0,
              limit: 5,
            }) as Array<{
            detail: string;
          }>
        ).map((row) => row.detail);

        assert.ok(
          details.some((detail) =>
            detail.includes(
              "SEARCH e USING INTEGER PRIMARY KEY",
            ),
          ),
          `${shape}: ${details.join(" | ")}`,
        );
        assert.equal(
          details.some(
            (detail) =>
              detail.trim() === "SCAN e",
          ),
          false,
          `${shape}: ${details.join(" | ")}`,
        );
      }
    } finally {
      db.close();
    }
  },
);

test(
  "v41-7 a 4.9 sent event and a post-migration claimed event share bridge_status history",
  (t) => {
    const messageId = randomUUID();
    const dbPath = makeV49Db(t, (db) => {
      insertCurrentMessage(db, messageId, {
        events: [
          {
            seq: 1,
            attemptId: null,
            event: "sent",
            at: ISO0,
            detail: null,
          },
        ],
      });
    });

    migrateBridgeDatabaseAtPath(dbPath);
    const bus = BridgeBus.open(dbPath);

    try {
      const fetched = bus.fetch(
        "codex",
        "codex:v41-cross-migration",
        {
          messageId,
          now: T0 + 1,
        },
      );
      assert.equal(
        fetched.messages[0]?.message_id,
        messageId,
      );
      assert.deepEqual(
        bus
          .status(messageId)
          .events.slice(0, 2)
          .map((event) => event.event),
        ["sent", "claimed"],
      );
    } finally {
      bus.close();
    }
  },
);

test(
  "v41-8 hook output and the sweep summary stay identical across migration",
  (t) => {
    const messageId = randomUUID();
    const dbPath = makeV49Db(t, (db) => {
      insertCurrentMessage(db, messageId, {
        fromRole: "codex",
        toRole: "claude",
      });
    });
    const declared = {
      tag: null,
      unusable: null,
    };

    const before = {
      hook: createHookOutput(
        "stop",
        countPendingClaudeMessages(
          dbPath,
          T0 + 1,
          declared,
        ),
      ),
      sweep: sweepSummary(
        dbPath,
        T0 + 1,
      ),
    };

    migrateBridgeDatabaseAtPath(dbPath);

    const after = {
      hook: createHookOutput(
        "stop",
        countPendingClaudeMessages(
          dbPath,
          T0 + 1,
          declared,
        ),
      ),
      sweep: sweepSummary(
        dbPath,
        T0 + 1,
      ),
    };

    assert.deepEqual(after, before);
  },
);