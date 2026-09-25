import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test, { type TestContext } from "node:test";
import Database from "better-sqlite3";
import {
  type EndpointMapping,
  SCHEMA_VERSION,
  initializeBridgeDatabaseAtPath,
  migrateBridgeDatabaseAtPath,
  sha256,
} from "../src/db.js";

type Sql = InstanceType<typeof Database>;

const STAMP = "2026-09-24T00:00:00.000Z";
const MAPPING: EndpointMapping = {
  endpoints: [
    { role: "claude", name: "claude-main" },
    { role: "codex", name: "codex-main" },
  ],
  tags: [
    { role: "claude", tag: null, endpoint: "claude-main" },
    { role: "codex", tag: null, endpoint: "codex-main" },
  ],
};

const V41_SCHEMA_SQL = `
CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT NOT NULL UNIQUE,
  root_id TEXT NOT NULL,
  from_role TEXT NOT NULL CHECK (from_role IN ('claude','codex')),
  to_role TEXT NOT NULL CHECK (to_role IN ('claude','codex')),
  to_tag TEXT, from_tag TEXT,
  on_timeout TEXT CHECK (on_timeout IS NULL OR on_timeout IN ('bounce','fallback')),
  tag_expires_at INTEGER,
  subject TEXT NOT NULL, body TEXT NOT NULL,
  envelope_sha256 TEXT NOT NULL, body_sha256 TEXT NOT NULL,
  sender_thread_id TEXT,
  status TEXT NOT NULL DEFAULT 'stored'
    CHECK (status IN ('stored','claimed','presented','acked','rejected','bounced')),
  attempt_id TEXT, consumer TEXT, lease_expires_at INTEGER,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  sent_at TEXT NOT NULL, presented_at TEXT, acked_at TEXT,
  CHECK (from_role <> to_role),
  CHECK (
    (to_tag IS NULL AND on_timeout IS NULL AND tag_expires_at IS NULL) OR
    (to_tag IS NOT NULL AND on_timeout IN ('bounce','fallback') AND tag_expires_at IS NOT NULL) OR
    (to_tag IS NOT NULL AND on_timeout IS NULL AND tag_expires_at IS NULL)
  )
);
CREATE INDEX idx_inbox ON messages (to_role, status, id);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id TEXT, attempt_id TEXT,
  event TEXT NOT NULL, at TEXT NOT NULL, detail TEXT
);
`;

const MESSAGES_4_13 = `
CREATE TABLE messages (
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

const IDENTITY_4_13 = `
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

function flatten(sql: string): string {
  return sql.replace(/"/g, "").replace(/\s+/g, " ").replace(/;/g, "").trim();
}

function withDb<T>(dbPath: string, work: (db: Sql) => T): T {
  const db = new Database(dbPath, { fileMustExist: true });
  try {
    return work(db);
  } finally {
    db.close();
  }
}

function readMeta(dbPath: string, key: string): string | null {
  return withDb(dbPath, (db) => {
    const row = db.prepare("SELECT v FROM meta WHERE k = ?").get(key) as
      | { v: string }
      | undefined;
    return row?.v ?? null;
  });
}

function countRows(dbPath: string, table: string): number {
  return withDb(
    dbPath,
    (db) =>
      (
        db.prepare(`SELECT COUNT(*) AS count FROM ${table}`).get() as {
          count: number;
        }
      ).count,
  );
}

function columnNames(dbPath: string, table: string): string[] {
  return withDb(dbPath, (db) =>
    (db.pragma(`table_info(${table})`) as Array<{ name: string }>).map(
      (row) => row.name,
    ),
  );
}

function envelopeOf(row: {
  from_role: string;
  subject: string;
  body: string;
  in_reply_to: string | null;
  reply_kind: string | null;
  expects_reply: number;
}): string {
  return sha256(
    JSON.stringify([
      2,
      row.from_role,
      row.subject,
      row.body,
      row.in_reply_to,
      row.reply_kind,
      row.expects_reply,
    ]),
  );
}

function schemaSignature(dbPath: string): string[] {
  return withDb(dbPath, (db) =>
    (
      db
        .prepare(
          `SELECT type, name, sql
             FROM sqlite_master
            WHERE sql IS NOT NULL
            ORDER BY type, name`,
        )
        .all() as Array<{ type: string; name: string; sql: string }>
    )
      .map(
        (row) =>
          `${row.type}:${row.name}:${row.sql
            .replace(/"/g, "")
            .replace(/\s+/g, " ")
            .replace(/\( /g, "(")
            .replace(/ \)/g, ")")
            .trim()}`,
      )
      .sort(),
  );
}

function temporaryDb(t: TestContext): string {
  const directory = mkdtempSync(join(tmpdir(), "agent-bridge-f-"));
  t.after(() => {
    rmSync(directory, { recursive: true, force: true });
  });
  return join(directory, "bridge.db");
}

function at(t: TestContext, stopAt: string): string {
  const dbPath = temporaryDb(t);
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  try {
    db.pragma("journal_mode = WAL");
    db.exec(V41_SCHEMA_SQL);
    const insert = db.prepare("INSERT INTO meta (k, v) VALUES (?, ?)");
    db.transaction(() => {
      insert.run("root_id", randomUUID());
      insert.run("schema_version", "4.1");
      insert.run("created_at", STAMP);
    }).immediate();
  } finally {
    db.close();
  }
  migrateBridgeDatabaseAtPath(dbPath, {
    mapping: MAPPING,
    stopAt,
    skipCutoverChecks: true,
  });
  return dbPath;
}

function fresh(t: TestContext): string {
  const dbPath = temporaryDb(t);
  initializeBridgeDatabaseAtPath(dbPath);
  return dbPath;
}

function insertCurrent(
  db: Sql,
  sourceId: string,
  row: {
    messageId: string;
    expectsReply?: number;
    inReplyTo?: string | null;
    replyKind?: string | null;
  },
): void {
  db.prepare(
    `INSERT INTO messages (
       message_id, from_role, source_endpoint_id, subject, body,
       envelope_sha256, envelope_version, body_sha256, sent_at,
       expects_reply, in_reply_to, reply_kind
     ) VALUES (?, 'claude', ?, 's', 'b', 'aa', 2, 'bb', ?, ?, ?, ?)`,
  ).run(
    row.messageId,
    sourceId,
    STAMP,
    row.expectsReply ?? 0,
    row.inReplyTo ?? null,
    row.replyKind ?? null,
  );
}

function withCurrent(
  t: TestContext,
  work: (db: Sql, sourceId: string) => void,
): void {
  const dbPath = fresh(t);
  const db = new Database(dbPath);
  try {
    db.pragma("foreign_keys = ON");
    const sourceId = randomUUID();
    db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at, retired_at
       ) VALUES (?, 'claude', 'src', ?, NULL)`,
    ).run(sourceId, STAMP);
    work(db, sourceId);
  } finally {
    db.close();
  }
}

test("f-m1: a 4.13 database migrates to 4.14 with defaults and the same envelope", (t) => {
  assert.equal(SCHEMA_VERSION, "4.14");
  const dbPath = at(t, "4.13");
  const busy = randomUUID();
  let messagesBeforeRows: Array<Record<string, unknown>> = [];
  withDb(dbPath, (db) => {
    const source = (
      db
        .prepare(
          "SELECT endpoint_id FROM endpoints WHERE role = 'claude' AND name = 'claude-main'",
        )
        .get() as { endpoint_id: string }
    ).endpoint_id;
    const codexMain = (
      db
        .prepare(
          "SELECT endpoint_id FROM endpoints WHERE role = 'codex' AND name = 'codex-main'",
        )
        .get() as { endpoint_id: string }
    ).endpoint_id;
    const addEndpoint = db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at, retired_at
       ) VALUES (?, 'codex', ?, ?, NULL)`,
    );
    const confirmedTo = randomUUID();
    const bouncedTo = randomUUID();
    addEndpoint.run(confirmedTo, "codex-b", STAMP);
    addEndpoint.run(bouncedTo, "codex-c", STAMP);
    const insert = db.prepare(
      `INSERT INTO messages (
         message_id, from_role, source_endpoint_id,
         subject, body, envelope_sha256, envelope_version,
         body_sha256, sender_thread_id, attempt_count, sent_at
       ) VALUES (?, 'claude', ?, ?, ?, ?, 2, ?, ?, 7, ?)`,
    );
    for (const [id, subject, body] of [
      [randomUUID(), "alpha", "body-alpha"],
      [randomUUID(), "beta", "body-beta"],
      [busy, "gamma", "body-gamma"],
    ] as const) {
      insert.run(
        id,
        source,
        subject,
        body,
        envelopeOf({
          from_role: "claude",
          subject,
          body,
          in_reply_to: null,
          reply_kind: null,
          expects_reply: 0,
        }),
        sha256(body),
        `thread-${subject}`,
        STAMP,
      );
    }
    messagesBeforeRows = db
      .prepare("SELECT * FROM messages ORDER BY id")
      .all() as Array<Record<string, unknown>>;
    const deliver = db.prepare(
      `INSERT INTO deliveries (
         message_id, endpoint_id, state, holder, attempt_id,
         attempt_count, lease_until, presented_at, confirmed_at
       ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)`,
    );
    deliver.run(busy, codexMain, "pending", null, null, null, null, null);
    deliver.run(
      busy,
      confirmedTo,
      "confirmed",
      "holder",
      randomUUID(),
      null,
      STAMP,
      STAMP,
    );
    deliver.run(busy, bouncedTo, "bounced", null, null, null, null, null);
  });
  const messagesBefore = countRows(dbPath, "messages");
  const deliveriesBefore = countRows(dbPath, "deliveries");
  assert.equal(messagesBefore, 3);
  assert.equal(deliveriesBefore, 3);

  migrateBridgeDatabaseAtPath(dbPath);

  assert.equal(readMeta(dbPath, "schema_version"), "4.14");
  assert.equal(countRows(dbPath, "messages"), messagesBefore);
  assert.equal(countRows(dbPath, "deliveries"), deliveriesBefore);
  assert.deepEqual(columnNames(dbPath, "messages").slice(-3), [
    "expects_reply",
    "in_reply_to",
    "reply_kind",
  ]);
  withDb(dbPath, (db) => {
    assert.equal(db.prepare("PRAGMA foreign_key_check").all().length, 0);
    const migratedRows = db
      .prepare("SELECT * FROM messages ORDER BY id")
      .all() as Array<Record<string, unknown>>;
    assert.deepEqual(
      migratedRows.map((row) => {
        const old = { ...row };
        delete old.expects_reply;
        delete old.in_reply_to;
        delete old.reply_kind;
        return old;
      }),
      messagesBeforeRows,
    );
    const rows = db
      .prepare(
        `SELECT message_id, from_role, subject, body,
                in_reply_to, reply_kind, expects_reply, envelope_sha256
           FROM messages
          ORDER BY id`,
      )
      .all() as Array<{
      message_id: string;
      from_role: string;
      subject: string;
      body: string;
      in_reply_to: string | null;
      reply_kind: string | null;
      expects_reply: number;
      envelope_sha256: string;
    }>;
    assert.equal(new Set(rows.map((row) => row.subject)).size, 3);
    assert.equal(new Set(rows.map((row) => row.body)).size, 3);
    for (const row of rows) {
      assert.equal(row.expects_reply, 0);
      assert.equal(row.in_reply_to, null);
      assert.equal(row.reply_kind, null);
      assert.equal(row.envelope_sha256, envelopeOf(row));
    }
    const states = (
      db
        .prepare(
          "SELECT state FROM deliveries WHERE message_id = ? ORDER BY state",
        )
        .all(busy) as Array<{ state: string }>
    ).map((row) => row.state);
    assert.deepEqual(states, ["bounced", "confirmed", "pending"]);
  });
});

test("f-m2: fresh 4.14 and a migrated 4.14 share messages DDL, triggers, and indexes", (t) => {
  const migrated = at(t, "4.13");
  migrateBridgeDatabaseAtPath(migrated);
  const created = fresh(t);
  const got = schemaSignature(migrated);
  const want = schemaSignature(created);
  for (const prefix of ["table:messages:", "trigger:", "index:"]) {
    assert.deepEqual(
      got.filter((row) => row.startsWith(prefix)),
      want.filter((row) => row.startsWith(prefix)),
    );
  }
});

test("f-m3: the 4.14 identity trigger refuses updates of the three new columns", (t) => {
  withCurrent(t, (db, sourceId) => {
    const messageId = randomUUID();
    insertCurrent(db, sourceId, { messageId });
    const read = () =>
      db
        .prepare(
          `SELECT expects_reply, in_reply_to, reply_kind
             FROM messages WHERE message_id = ?`,
        )
        .get(messageId);
    const before = read();
    assert.throws(
      () =>
        db
          .prepare(
            "UPDATE messages SET expects_reply = 1 WHERE message_id = ?",
          )
          .run(messageId),
      /message identity is immutable/,
    );
    assert.throws(
      () =>
        db
          .prepare(
            "UPDATE messages SET in_reply_to = ? WHERE message_id = ?",
          )
          .run(messageId, messageId),
      /message identity is immutable/,
    );
    assert.throws(
      () =>
        db
          .prepare(
            "UPDATE messages SET reply_kind = 'answer' WHERE message_id = ?",
          )
          .run(messageId),
      /message identity is immutable/,
    );
    assert.deepEqual(read(), before);
  });
});

test("f-m4: migrating a 4.12 database with target 4.13 keeps the frozen messages DDL", (t) => {
  const dbPath = at(t, "4.12");
  assert.equal(readMeta(dbPath, "schema_version"), "4.12");
  migrateBridgeDatabaseAtPath(dbPath, {
    mapping: MAPPING,
    stopAt: "4.13",
    skipCutoverChecks: true,
  });
  assert.equal(readMeta(dbPath, "schema_version"), "4.13");
  const columns = columnNames(dbPath, "messages");
  for (const name of [
    "expects_reply",
    "in_reply_to",
    "reply_kind",
    "to_role",
  ]) {
    assert.equal(columns.includes(name), false, name);
  }
  assert.equal(columns.includes("legacy_from_tag"), true);
  const schema = withDb(dbPath, (db) =>
    db.prepare(
      `SELECT type, sql FROM sqlite_master
        WHERE name IN ('messages', 'messages_identity_immutable')
        ORDER BY type`,
    ).all() as Array<{ type: string; sql: string }>,
  );
  assert.equal(schema.length, 2);
  assert.deepEqual(
    schema.map((row) => [row.type, flatten(row.sql)]),
    [
      ["table", flatten(MESSAGES_4_13)],
      ["trigger", flatten(IDENTITY_4_13)],
    ],
  );
});

test("f-m5: 4.14 CHECKs and the in_reply_to foreign key reject illegal rows", (t) => {
  withCurrent(t, (db, sourceId) => {
    assert.equal(Number(db.pragma("foreign_keys", { simple: true })), 1);
    const parent = randomUUID();
    insertCurrent(db, sourceId, { messageId: parent });
    insertCurrent(db, sourceId, {
      messageId: randomUUID(),
      inReplyTo: parent,
      replyKind: "answer",
    });
    assert.throws(
      () =>
        insertCurrent(db, sourceId, {
          messageId: randomUUID(),
          replyKind: "answer",
        }),
      /CHECK constraint failed/,
    );
    assert.throws(
      () =>
        insertCurrent(db, sourceId, {
          messageId: randomUUID(),
          inReplyTo: parent,
        }),
      /CHECK constraint failed/,
    );
    assert.throws(
      () =>
        insertCurrent(db, sourceId, {
          messageId: randomUUID(),
          inReplyTo: parent,
          replyKind: "decline",
          expectsReply: 1,
        }),
      /CHECK constraint failed/,
    );
    assert.throws(
      () =>
        insertCurrent(db, sourceId, {
          messageId: randomUUID(),
          inReplyTo: "missing-message",
          replyKind: "withdraw",
        }),
      /FOREIGN KEY constraint failed/,
    );
    assert.equal(
      (
        db.prepare("SELECT COUNT(*) AS count FROM messages").get() as {
          count: number;
        }
      ).count,
      2,
    );
  });
});