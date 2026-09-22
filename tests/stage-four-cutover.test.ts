import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import {
  BridgeBus,
  type EndpointMapping,
  MIGRATION_PAUSE_ENV,
  MIGRATION_STEPS,
  SCHEMA_VERSION,
  initializeBridgeDatabaseAtPath,
  lostQuerySql,
  migrateBridgeDatabaseAtPath,
  planMigration,
  readServerForeignKeys,
  sha256,
} from "../src/db.js";
import "../src/bridge-init.js";

const PROJECT_ROOT = resolve(
  fileURLToPath(new URL("..", import.meta.url)),
);
const INIT_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-init.ts",
);
const CREATED_AT = "2026-09-06T00:00:00.000Z";

const MAPPING: EndpointMapping = {
  endpoints: [
    { role: "claude", name: "claude-main" },
    { role: "codex", name: "codex-main" },
  ],
  tags: [
    { role: "claude", tag: null, endpoint: "claude-main" },
    { role: "codex", tag: null, endpoint: "codex-main" },
    { role: "claude", tag: "lane", endpoint: "claude-main" },
    { role: "codex", tag: "src", endpoint: "codex-main" },
  ],
};

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

function baseConfig(): Record<string, unknown> {
  return {
    mcpServers: {
      bridge: {
        command: "node",
        args: [
          "C:/agent-bridge/dist/server.js",
          "--role",
          "codex",
          "--endpoint",
          "codex-main",
        ],
      },
      bridgeClaude: {
        command: "node",
        args: [
          "C:/agent-bridge/dist/server.js",
          "--role",
          "claude",
          "--endpoint",
          "claude-main",
        ],
      },
    },
    hooks: {
      Stop: [
        {
          command: "node",
          args: [
            "C:/agent-bridge/dist/hook-notify.js",
            "--event",
            "stop",
          ],
          env: {
            AGENT_BRIDGE_ENDPOINT: "claude-main",
          },
        },
      ],
    },
  };
}

function withDb<T>(
  dbPath: string,
  work: (db: InstanceType<typeof Database>) => T,
): T {
  const db = new Database(dbPath, { fileMustExist: true });
  try {
    return work(db);
  } finally {
    db.close();
  }
}

function makeProfile(
  t: TestContext,
  prefix: string,
): { userProfile: string; dbPath: string } {
  const userProfile = mkdtempSync(join(tmpdir(), prefix));
  t.after(() => {
    rmSync(userProfile, { recursive: true, force: true });
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

function writeVersionDb(
  dbPath: string,
  version: "3.2" | "4.1",
): void {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new Database(dbPath);
  try {
    db.pragma("journal_mode = WAL");
    db.exec(version === "3.2" ? V32_SCHEMA_SQL : V41_SCHEMA_SQL);
    const insert = db.prepare(
      "INSERT INTO meta (k, v) VALUES (?, ?)",
    );
    insert.run("root_id", randomUUID());
    insert.run("schema_version", version);
    insert.run("created_at", CREATED_AT);
  } finally {
    db.close();
  }
}

function seedMessage(
  dbPath: string,
  version: "3.2" | "4.1",
  tag: string | null,
): void {
  withDb(dbPath, (db) => {
    const root = (
      db.prepare("SELECT v FROM meta WHERE k = ?").get("root_id") as {
        v: string;
      }
    ).v;
    const body = "body";
    const hash = sha256(body);
    if (version === "3.2" || tag === null) {
      db.prepare(
        `INSERT INTO messages (
           message_id, root_id, from_role, to_role,
           subject, body, envelope_sha256, body_sha256, sent_at
         ) VALUES (?, ?, 'codex', 'claude', 'subject', ?, ?, ?, ?)`,
      ).run(randomUUID(), root, body, hash, hash, CREATED_AT);
      return;
    }
    db.prepare(
      `INSERT INTO messages (
         message_id, root_id, from_role, to_role,
         to_tag, from_tag, on_timeout, tag_expires_at,
         subject, body, envelope_sha256, body_sha256, sent_at
       ) VALUES (
         ?, ?, 'codex', 'claude',
         ?, 'src', 'bounce', 1,
         'subject', ?, ?, ?, ?
       )`,
    ).run(randomUUID(), root, tag, body, hash, hash, CREATED_AT);
  });
}

function writeJson(
  directory: string,
  name: string,
  value: unknown,
): string {
  const path = join(directory, name);
  writeFileSync(path, JSON.stringify(value), "utf8");
  return path;
}

function readMeta(dbPath: string, key: string): string | null {
  return withDb(dbPath, (db) => {
    const row = db
      .prepare("SELECT v FROM meta WHERE k = ?")
      .get(key) as { v: string } | undefined;
    return row?.v ?? null;
  });
}

function tableColumns(dbPath: string, table: string): string[] {
  return withDb(dbPath, (db) =>
    (
      db.pragma(`table_info(${table})`) as Array<{ name: string }>
    ).map((row) => row.name),
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
        .all() as Array<{
        type: string;
        name: string;
        sql: string;
      }>
    )
      .map(
        /*
         * ALTER TABLE ... RENAME rewrites the table name in double quotes,
         * so a migrated schema reads `CREATE TABLE "messages"` where a
         * fresh one reads `CREATE TABLE messages`. The quotes carry no
         * meaning; strip them before comparing.
         */
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

function parkAt410(
  t: TestContext,
  prefix: string,
  seed: "none" | "untagged" | "tagged" | "gone",
): { userProfile: string; dbPath: string } {
  const fixture = makeProfile(t, prefix);
  writeVersionDb(fixture.dbPath, "4.1");
  if (seed === "untagged") {
    seedMessage(fixture.dbPath, "4.1", null);
  } else if (seed === "tagged") {
    seedMessage(fixture.dbPath, "4.1", "lane");
  } else if (seed === "gone") {
    seedMessage(fixture.dbPath, "4.1", "gone");
  }
  migrateBridgeDatabaseAtPath(fixture.dbPath, {
    stopAt: "4.10",
  });
  /*
   * Settle the WAL into the main file. A refused cutover opens the file
   * once more (for its backup), and the checkpoint that open performs
   * would move committed frames into the main file and make a byte
   * comparison read as a change that never was one.
   */
  withDb(fixture.dbPath, (db) => {
    db.pragma("wal_checkpoint(TRUNCATE)");
  });
  return fixture;
}

function guardOptions(configPath: string): {
  mapping: EndpointMapping;
  configPaths: string[];
} {
  return { mapping: MAPPING, configPaths: [configPath] };
}

function refusalLines(error: unknown): string[] {
  const message = error instanceof Error ? error.message : String(error);
  return message.split("\n").filter((line) => line.startsWith("precheck "));
}

function assertOnly(
  lines: string[],
  id: string,
  status: "NG" | "未確認",
): void {
  assert.equal(lines.length, 6, lines.join("\n"));
  for (const line of lines) {
    const match = /^precheck ([^:]+): (\S+) /.exec(line);
    assert.ok(match, line);
    if (match[1] === id) {
      assert.equal(match[2], status, line);
    } else {
      assert.equal(match[2], "OK", line);
    }
  }
}

function assertArrived(dbPath: string): void {
  assert.equal(readMeta(dbPath, "schema_version"), "4.13");
  withDb(dbPath, (db) => {
    const nullEndpoints = (
      db
        .prepare(
          "SELECT COUNT(*) AS count FROM deliveries WHERE endpoint_id IS NULL",
        )
        .get() as { count: number }
    ).count;
    const nullSources = (
      db
        .prepare(
          "SELECT COUNT(*) AS count FROM messages WHERE source_endpoint_id IS NULL",
        )
        .get() as { count: number }
    ).count;
    const messages = (
      db.prepare("SELECT COUNT(*) AS count FROM messages").get() as {
        count: number;
      }
    ).count;
    assert.equal(nullEndpoints, 0);
    assert.equal(nullSources, 0);
    assert.ok(messages > 0);
    assert.equal(db.prepare("PRAGMA foreign_key_check").all().length, 0);
  });
}

/*
 * Live bridge servers run on the machine that runs this suite; check 1
 * would refuse every cutover. The seam is read per call in bridge-init.
 */
process.env.AGENT_BRIDGE_TEST_PROCESS_SCAN = "quiet";

test("b-1: a failing cutover check does not change the database, and 4.1 plans four steps", async (t) => {
  const plan = planMigration("4.1").map(
    (step) => `${step.from}->${step.to}`,
  );
  for (const edge of [
    "4.9->4.10",
    "4.10->4.11",
    "4.11->4.12",
    "4.12->4.13",
  ]) {
    assert.ok(plan.includes(edge), plan.join(","));
  }
  const broken = MIGRATION_STEPS.flatMap((step) => {
    if (step.from === "4.9") {
      return [{ ...step, to: SCHEMA_VERSION }];
    }
    if (
      step.from === "4.10" ||
      step.from === "4.11" ||
      step.from === "4.12"
    ) {
      return [];
    }
    return [step];
  });
  assert.deepEqual(
    planMigration("4.9", broken).map(
      (step) => `${step.from}->${step.to}`,
    ),
    [`4.9->${SCHEMA_VERSION}`],
  );

  const quiet = parkAt410(t, "b1-quiet-", "untagged");
  const configPath = writeJson(
    quiet.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  const before = readFileSync(quiet.dbPath);
  assert.throws(
    () => migrateBridgeDatabaseAtPath(quiet.dbPath, { mapping: MAPPING }),
    /migration refused by precheck/,
  );
  /*
   * The refused run still took its backup, whose name carries a
   * one-second stamp and is never overwritten; a retry inside the same
   * second is refused for the name, not by a check. Wait it out.
   */
  await new Promise((resolve) => setTimeout(resolve, 1100));
  const missingConfig = refusalLines(
    (() => {
      try {
        migrateBridgeDatabaseAtPath(quiet.dbPath, { mapping: MAPPING });
      } catch (error) {
        return error;
      }
      return new Error("missing config was accepted");
    })(),
  );
  assert.match(
    missingConfig.find((line) => line.startsWith("precheck 2a:")) ?? "",
    /未確認/,
  );
  assert.match(
    missingConfig.find((line) => line.startsWith("precheck 2b:")) ?? "",
    /未確認/,
  );
  assert.deepEqual(readFileSync(quiet.dbPath), before);

  const cases: Array<{
    name: string;
    id: string;
    status: "NG" | "未確認";
    prepare: (fixture: { userProfile: string; dbPath: string }) => string;
  }> = [
    {
      name: "check-1",
      id: "1",
      status: "NG",
      prepare: (fixture) => {
        withDb(fixture.dbPath, (db) => {
          db.prepare(
            `UPDATE deliveries
                SET state = 'leased',
                    holder = 'holder',
                    attempt_id = ?,
                    lease_until = ?`,
          ).run(randomUUID(), Date.now() + 60_000);
        });
        return writeJson(fixture.userProfile, "config.json", baseConfig());
      },
    },
    {
      name: "check-1b",
      id: "1b",
      status: "NG",
      prepare: (fixture) =>
        writeJson(fixture.userProfile, "config.json", baseConfig()),
    },
    {
      name: "check-2a",
      id: "2a",
      status: "NG",
      prepare: (fixture) =>
        writeJson(fixture.userProfile, "config.json", {
          note: "to_tag",
          ...baseConfig(),
        }),
    },
    {
      name: "check-2b",
      id: "2b",
      status: "NG",
      prepare: (fixture) =>
        writeJson(fixture.userProfile, "config.json", {
          mcpServers: {
            bridge: {
              command: "node",
              args: [
                "C:/agent-bridge/dist/server.js",
                "--role",
                "codex",
              ],
            },
          },
        }),
    },
    {
      name: "check-3",
      id: "3",
      status: "NG",
      prepare: (fixture) =>
        writeJson(fixture.userProfile, "config.json", baseConfig()),
    },
    {
      name: "check-4",
      id: "4",
      status: "NG",
      prepare: (fixture) => {
        writeFileSync(
          `${fixture.dbPath}.pre-9-99999999-999999`,
          "nope",
          "utf8",
        );
        return writeJson(fixture.userProfile, "config.json", baseConfig());
      },
    },
  ];

  for (const candidate of cases) {
    const seed = candidate.id === "3" ? "gone" : "untagged";
    const fixture = parkAt410(t, `b1-${candidate.name}-`, seed);
    /*
     * Snapshot after the arrangement: check 1's case writes the lock row
     * into the database on purpose, and that write is the fixture, not
     * the migration.
     */
    const path = candidate.prepare(fixture);
    const bytes = readFileSync(fixture.dbPath);
    const options =
      candidate.id === "1b"
        ? { ...guardOptions(path), stopAt: "4.12" }
        : guardOptions(path);
    let caught: unknown;
    assert.throws(() => {
      try {
        migrateBridgeDatabaseAtPath(fixture.dbPath, options);
      } catch (error) {
        caught = error;
        throw error;
      }
    }, /migration refused by precheck/);
    assertOnly(refusalLines(caught), candidate.id, candidate.status);
    assert.deepEqual(readFileSync(fixture.dbPath), bytes);
    assert.equal(readMeta(fixture.dbPath, "schema_version"), "4.10");
  }
});

test("b-2: 3.2, 4.1, and 4.10 reach 4.13, and a broken reference does not", (t) => {
  const from32 = makeProfile(t, "b2-32-");
  writeVersionDb(from32.dbPath, "3.2");
  seedMessage(from32.dbPath, "3.2", null);
  migrateBridgeDatabaseAtPath(from32.dbPath, { mapping: MAPPING });
  assertArrived(from32.dbPath);

  const from41 = makeProfile(t, "b2-41-");
  writeVersionDb(from41.dbPath, "4.1");
  seedMessage(from41.dbPath, "4.1", null);
  migrateBridgeDatabaseAtPath(from41.dbPath, { mapping: MAPPING });
  assertArrived(from41.dbPath);

  const from410 = parkAt410(t, "b2-410-", "untagged");
  const configPath = writeJson(
    from410.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  migrateBridgeDatabaseAtPath(from410.dbPath, guardOptions(configPath));
  assertArrived(from410.dbPath);

  const broken = parkAt410(t, "b2-fk-", "untagged");
  const bogus = randomUUID();
  withDb(broken.dbPath, (db) => {
    db.pragma("foreign_keys = OFF");
    db.prepare(
      "UPDATE deliveries SET endpoint_id = ? WHERE endpoint_id IS NULL",
    ).run(bogus);
  });
  const brokenConfig = writeJson(
    broken.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  assert.throws(
    () =>
      migrateBridgeDatabaseAtPath(
        broken.dbPath,
        guardOptions(brokenConfig),
      ),
    /PRAGMA foreign_key_check failed/,
  );
  assert.equal(readMeta(broken.dbPath, "schema_version"), "4.10");
  withDb(broken.dbPath, (db) => {
    const row = db
      .prepare("SELECT endpoint_id FROM deliveries")
      .get() as { endpoint_id: string };
    assert.equal(row.endpoint_id, bogus);
  });
});

test("b-3: each cutover step fails on its own and leaves the version", (t) => {
  const missingTag = parkAt410(t, "b3-tag-", "gone");
  assert.throws(
    () =>
      migrateBridgeDatabaseAtPath(missingTag.dbPath, {
        mapping: MAPPING,
        stopAt: "4.11",
        skipCutoverChecks: true,
      }),
    /no mapping for delivery tag role=claude tag="gone"/,
  );
  assert.equal(readMeta(missingTag.dbPath, "schema_version"), "4.10");

  const skipFill = parkAt410(t, "b3-notnull-", "untagged");
  withDb(skipFill.dbPath, (db) => {
    db.prepare(
      "UPDATE meta SET v = '4.11' WHERE k = 'schema_version'",
    ).run();
  });
  assert.throws(
    () =>
      migrateBridgeDatabaseAtPath(skipFill.dbPath, {
        stopAt: "4.12",
        skipCutoverChecks: true,
      }),
    /NOT NULL constraint failed/,
  );
  assert.equal(readMeta(skipFill.dbPath, "schema_version"), "4.11");

  const source = parkAt410(t, "b3-source-", "untagged");
  migrateBridgeDatabaseAtPath(source.dbPath, {
    mapping: MAPPING,
    stopAt: "4.12",
    skipCutoverChecks: true,
  });
  assert.equal(readMeta(source.dbPath, "schema_version"), "4.12");
  const noSource: EndpointMapping = {
    endpoints: MAPPING.endpoints,
    tags: MAPPING.tags.filter(
      (tag) => !(tag.role === "codex" && tag.tag === null),
    ),
  };
  assert.throws(
    () =>
      migrateBridgeDatabaseAtPath(source.dbPath, {
        mapping: noSource,
        skipCutoverChecks: true,
      }),
    /no default mapping for untagged source role=codex/,
  );
  assert.equal(readMeta(source.dbPath, "schema_version"), "4.12");
});

test("b-4: fresh 4.13 and a migrated 4.13 share sqlite_master, and the server keeps foreign keys", (t) => {
  const fresh = makeProfile(t, "b4-fresh-");
  initializeBridgeDatabaseAtPath(fresh.dbPath);
  const migrated = parkAt410(t, "b4-migrated-", "untagged");
  const configPath = writeJson(
    migrated.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  migrateBridgeDatabaseAtPath(migrated.dbPath, guardOptions(configPath));
  assert.deepEqual(
    schemaSignature(migrated.dbPath),
    schemaSignature(fresh.dbPath),
  );

  const dropped = [
    "to_role",
    "to_tag",
    "on_timeout",
    "tag_expires_at",
    "status",
    "attempt_id",
    "consumer",
    "lease_expires_at",
    "presented_at",
    "acked_at",
    "from_tag",
    "root_id",
  ];
  const columns = tableColumns(fresh.dbPath, "messages");
  for (const name of dropped) {
    assert.equal(columns.includes(name), false, name);
  }
  assert.equal(columns.includes("legacy_from_tag"), true);
  assert.equal(columns.includes("attempt_count"), true);
  assert.equal(columns.includes("source_endpoint_id"), true);

  const names = schemaSignature(fresh.dbPath).map(
    (row) => row.split(":")[1],
  );
  assert.equal(names.includes("deliveries_one_per_message"), false);
  assert.equal(names.includes("idx_inbox"), false);
  assert.equal(names.includes("idx_deliveries_endpoint_state"), true);
  assert.equal(names.includes("message_events"), true);
  assert.equal(
    schemaSignature(fresh.dbPath).filter((row) =>
      row.startsWith("trigger:"),
    ).length,
    5,
  );
  const trigger = schemaSignature(fresh.dbPath).find((row) =>
    row.startsWith("trigger:messages_identity_immutable:"),
  );
  assert.ok(trigger);
  for (const column of [
    "message_id",
    "from_role",
    "source_endpoint_id",
    "legacy_to_tag",
    "legacy_from_tag",
    "subject",
    "body",
    "envelope_sha256",
    "envelope_version",
  ]) {
    assert.match(trigger, new RegExp(`\\b${column}\\b`));
  }
  const deliveries = schemaSignature(fresh.dbPath).find((row) =>
    row.startsWith("table:deliveries:"),
  );
  assert.match(deliveries ?? "", /UNIQUE \(message_id, endpoint_id\)/);
  assert.match(deliveries ?? "", /endpoint_id TEXT NOT NULL/);

  withDb(migrated.dbPath, (db) => {
    assert.throws(
      () =>
        db
          .prepare(
            "UPDATE messages SET legacy_from_tag = 'changed'",
          )
          .run(),
      /message identity is immutable/,
    );
    assert.throws(
      () =>
        db.prepare("UPDATE messages SET message_id = ?").run(randomUUID()),
      /message identity is immutable/,
    );
    const sent = db
      .prepare("UPDATE messages SET sent_at = ?")
      .run("2026-09-07T00:00:00.000Z");
    assert.equal(sent.changes, 1);
  });
  assert.equal(readServerForeignKeys(fresh.dbPath), 1);
});

test("b-5: a tagged message keeps legacy_to_tag and legacy_from_tag", (t) => {
  const fixture = makeProfile(t, "b5-");
  writeVersionDb(fixture.dbPath, "4.1");
  seedMessage(fixture.dbPath, "4.1", "lane");
  migrateBridgeDatabaseAtPath(fixture.dbPath, { mapping: MAPPING });
  withDb(fixture.dbPath, (db) => {
    const row = db
      .prepare(
        `SELECT legacy_to_tag, legacy_from_tag
           FROM messages`,
      )
      .get() as {
      legacy_to_tag: string | null;
      legacy_from_tag: string | null;
    };
    assert.equal(row.legacy_to_tag, "lane");
    assert.equal(row.legacy_from_tag, "src");
    const columns = (
      db.pragma("table_info(messages)") as Array<{ name: string }>
    ).map((column) => column.name);
    assert.equal(columns.includes("to_tag"), false);
    assert.equal(columns.includes("from_tag"), false);
  });
});

test("b-6: lostQuerySql returns subject, deadEndpoint, and bounceTo for one bounced delivery", (t) => {
  const sql = lostQuerySql();
  assert.match(sql.page, /dead\.name AS deadEndpoint/);
  assert.match(sql.page, /src\.name  *AS bounceTo/);
  for (const text of [sql.page, sql.count]) {
    assert.match(text, /JOIN message_events e/);
    assert.match(text, /d\.delivery_id = e\.delivery_id/);
    for (const column of [
      "to_role",
      "status",
      "to_tag",
      "from_tag",
      "on_timeout",
      "tag_expires_at",
    ]) {
      assert.equal(
        new RegExp(`\\b${column}\\b`).test(text),
        false,
        column,
      );
    }
  }

  const fixture = makeProfile(t, "b6-");
  initializeBridgeDatabaseAtPath(fixture.dbPath);
  withDb(fixture.dbPath, (db) => {
    const source = randomUUID();
    const dead = randomUUID();
    const other = randomUUID();
    const messageId = randomUUID();
    const stamp = "2026-09-06T00:00:00.000Z";
    db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at, retired_at
       ) VALUES (?, 'claude', 'source', ?, NULL)`,
    ).run(source, stamp);
    db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at, retired_at
       ) VALUES (?, 'codex', 'dead', ?, NULL)`,
    ).run(dead, stamp);
    db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at, retired_at
       ) VALUES (?, 'codex', 'other', ?, NULL)`,
    ).run(other, stamp);
    db.prepare(
      `INSERT INTO messages (
         message_id, from_role, source_endpoint_id,
         subject, body, envelope_sha256, envelope_version,
         body_sha256, attempt_count, sent_at
       ) VALUES (?, 'claude', ?, 'lost subject', 'body', 'aa', 2, 'bb', 0, ?)`,
    ).run(messageId, source, stamp);
    const bounced = db
      .prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state, holder, attempt_id,
           attempt_count, lease_until, presented_at, confirmed_at
         ) VALUES (?, ?, 'bounced', NULL, NULL, 0, NULL, NULL, NULL)`,
      )
      .run(messageId, dead);
    db.prepare(
      `INSERT INTO deliveries (
         message_id, endpoint_id, state, holder, attempt_id,
         attempt_count, lease_until, presented_at, confirmed_at
       ) VALUES (?, ?, 'pending', NULL, NULL, 0, NULL, NULL, NULL)`,
    ).run(messageId, other);
    db.prepare(
      `INSERT INTO events (delivery_id, event, at)
       VALUES (?, 'bounced', ?)`,
    ).run(bounced.lastInsertRowid, stamp);
    const page = db.prepare(sql.page).all({
      role: "codex",
      since: 0,
      limit: 10,
    }) as Array<{
      subject: string;
      bounceTo: string | null;
      deadEndpoint: string | null;
      seq: number;
    }>;
    assert.equal(page.length, 1);
    assert.equal(page[0]?.subject, "lost subject");
    assert.equal(page[0]?.deadEndpoint, "dead");
    assert.equal(page[0]?.bounceTo, "source");
    const count = db.prepare(sql.count).get({
      role: "codex",
      since: 0,
    }) as { count: number };
    assert.equal(count.count, 1);
  });
});

test("b-15: lostQuerySql seeks the events primary key, and the endpoint index serves pending lookup", (t) => {
  const fixture = makeProfile(t, "b15-");
  initializeBridgeDatabaseAtPath(fixture.dbPath);
  withDb(fixture.dbPath, (db) => {
    for (const [shape, text] of Object.entries(lostQuerySql())) {
      const details = (
        db.prepare(`EXPLAIN QUERY PLAN ${text}`).all({
          role: "claude",
          since: 0,
          limit: 5,
        }) as Array<{ detail: string }>
      ).map((row) => row.detail);
      assert.ok(
        details.some((detail) =>
          detail.includes("SEARCH e USING INTEGER PRIMARY KEY"),
        ),
        `${shape}: ${details.join(" | ")}`,
      );
      assert.equal(
        details.some((detail) => detail.trim() === "SCAN e"),
        false,
        `${shape}: ${details.join(" | ")}`,
      );
    }

    const lookup = `SELECT delivery_id
                      FROM deliveries
                     WHERE endpoint_id = ?
                       AND state = 'pending'
                     ORDER BY delivery_id`;
    const indexed = (
      db.prepare(`EXPLAIN QUERY PLAN ${lookup}`).all("endpoint") as Array<{
        detail: string;
      }>
    ).map((row) => row.detail);
    assert.ok(
      indexed.some((detail) =>
        detail.includes("idx_deliveries_endpoint_state"),
      ),
      indexed.join(" | "),
    );
    db.exec("DROP INDEX idx_deliveries_endpoint_state");
    const dropped = (
      db.prepare(`EXPLAIN QUERY PLAN ${lookup}`).all("endpoint") as Array<{
        detail: string;
      }>
    ).map((row) => row.detail);
    assert.equal(
      dropped.some((detail) =>
        detail.includes("idx_deliveries_endpoint_state"),
      ),
      false,
      dropped.join(" | "),
    );
  });
});

test("b-16: BridgeBus.open refuses a database whose schema_version is not 4.13", (t) => {
  const current = makeProfile(t, "b16-current-");
  initializeBridgeDatabaseAtPath(current.dbPath);
  const opened = BridgeBus.open(current.dbPath);
  opened.close();
  withDb(current.dbPath, (db) => {
    db.prepare(
      "UPDATE meta SET v = '4.10' WHERE k = 'schema_version'",
    ).run();
  });
  assert.throws(
    () => BridgeBus.open(current.dbPath),
    /unsupported schema_version 4\.10; expected 4\.13/,
  );

  const old = parkAt410(t, "b16-old-", "none");
  assert.equal(readMeta(old.dbPath, "schema_version"), "4.10");
  assert.throws(
    () => BridgeBus.open(old.dbPath),
    /unsupported schema_version 4\.10; expected 4\.13/,
  );
});

test("b-17: rehearse prints N=2 and the four buckets without changing the live database", async (t) => {
  const live = parkAt410(t, "b17-live-", "untagged");
  const configPath = writeJson(
    live.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  const mappingPath = writeJson(
    live.userProfile,
    "mapping.json",
    MAPPING,
  );
  const beforeBytes = readFileSync(live.dbPath);
  const beforeMtime = statSync(live.dbPath).mtimeMs;
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      INIT_ENTRY,
      "--rehearse",
      "--mapping",
      mappingPath,
      "--config",
      configPath,
    ],
    {
      cwd: PROJECT_ROOT,
      env: { ...process.env, USERPROFILE: live.userProfile },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  const [code] = (await once(child, "close")) as [number | null];
  assert.equal(code, 0, stderr);
  for (const line of [
    "rehearse n2: A=confirmed B=pending",
    "rehearse pending_here=3",
    "rehearse pending_elsewhere=1",
    "rehearse expired_leased=1",
    "rehearse expired_presented=1",
  ]) {
    assert.match(stderr, new RegExp(line));
  }
  assert.equal(readMeta(live.dbPath, "schema_version"), "4.10");
  assert.deepEqual(readFileSync(live.dbPath), beforeBytes);
  assert.equal(statSync(live.dbPath).mtimeMs, beforeMtime);

  const paused = parkAt410(t, "b17-pause-", "none");
  const pausedConfig = writeJson(
    paused.userProfile,
    "operator-config.json",
    baseConfig(),
  );
  const pausedMapping = writeJson(
    paused.userProfile,
    "mapping.json",
    MAPPING,
  );
  const killer = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      INIT_ENTRY,
      "--migrate",
      "--mapping",
      pausedMapping,
      "--config",
      pausedConfig,
    ],
    {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        USERPROFILE: paused.userProfile,
        [MIGRATION_PAUSE_ENV]: "1",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  killer.stdout.resume();
  killer.stderr.setEncoding("utf8");
  let pausedErr = "";
  await new Promise<void>((resolvePause, rejectPause) => {
    const timer = setTimeout(() => {
      rejectPause(new Error(`pause timeout: ${pausedErr}`));
    }, 30_000);
    killer.stderr.on("data", (chunk: string) => {
      pausedErr += chunk;
      if (pausedErr.includes("migration paused after destructive DDL")) {
        clearTimeout(timer);
        resolvePause();
      }
    });
    killer.once("close", () => {
      clearTimeout(timer);
      rejectPause(new Error(`exited before pause: ${pausedErr}`));
    });
  });
  killer.kill();
  await once(killer, "close");
  assert.equal(readMeta(paused.dbPath, "schema_version"), "4.10");
  const backup = readdirSync(dirname(paused.dbPath))
    .filter((name) =>
      name.startsWith(`${paused.dbPath.split(/[/\\]/).pop()}.pre-4.10-`),
    )
    .sort()
    .at(-1);
  assert.ok(backup);
  rmSync(`${paused.dbPath}-wal`, { force: true });
  rmSync(`${paused.dbPath}-shm`, { force: true });
  copyFileSync(join(dirname(paused.dbPath), backup), paused.dbPath);
  assert.equal(readMeta(paused.dbPath, "schema_version"), "4.10");
  assert.throws(
    () => BridgeBus.open(paused.dbPath),
    /unsupported schema_version 4\.10/,
  );
  await new Promise((resolveWait) => setTimeout(resolveWait, 1100));
  migrateBridgeDatabaseAtPath(
    paused.dbPath,
    guardOptions(pausedConfig),
  );
  assert.equal(readMeta(paused.dbPath, "schema_version"), "4.13");
  const restored = BridgeBus.open(paused.dbPath);
  restored.close();
});

const DOC_CHECK_ENTRY = join(PROJECT_ROOT, "src", "doc-check.ts");
const TSX_LOADER = pathToFileURL(
  join(PROJECT_ROOT, "node_modules", "tsx", "dist", "loader.mjs"),
).href;
const FORBID = [
  "bridge_hello",
  "to_tag",
  "from_tag",
  "broadcast",
  "on_timeout",
  "require_tag",
  "strict_addressing",
  "to_endpoint",
];

function git(cwd: string, args: readonly string[]): void {
  execFileSync("git", args, { cwd, stdio: "ignore" });
}

function commitOperationalMirror(
  t: TestContext,
  files: Readonly<Record<string, string>>,
): string {
  const root = mkdtempSync(join(tmpdir(), "agent-bridge-b14-"));
  t.after(() => {
    rmSync(root, { recursive: true, force: true });
  });
  mkdirSync(join(root, "docs"), { recursive: true });
  mkdirSync(join(root, "src"), { recursive: true });
  for (const [name, body] of Object.entries(files)) {
    const full = join(root, name);
    mkdirSync(dirname(full), { recursive: true });
    writeFileSync(full, body);
  }
  /*
   * deploy.md names src/db.ts. The reference check holds operational
   * docs to tracked paths, so the mirror needs that file and nothing else.
   */
  if (!files["src/db.ts"]) {
    writeFileSync(join(root, "src", "db.ts"), "export {}\n");
  }
  git(root, ["init"]);
  git(root, ["config", "user.email", "t@example.com"]);
  git(root, ["config", "user.name", "t"]);
  git(root, ["add", "."]);
  git(root, ["commit", "-m", "docs"]);
  return root;
}

function runDocCheck(
  cwd: string,
  args: readonly string[],
): Promise<{ code: number | null; stderr: string }> {
  const child = spawn(
    process.execPath,
    ["--import", TSX_LOADER, DOC_CHECK_ENTRY, ...args],
    { cwd, stdio: ["ignore", "pipe", "pipe"] },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  return new Promise((resolveRun, rejectRun) => {
    child.once("error", rejectRun);
    child.once("close", (code) => {
      resolveRun({ code, stderr });
    });
  });
}

function agentsMdBody(deploy: string): string {
  const normalized = deploy.replace(/\r\n/g, "\n");
  const match = normalized.match(
    /<!--\s*canonical:\s*agents-md\s*-->\s*\n```[\w]*\n([\s\S]*?)\n```/,
  );
  assert.ok(match?.[1], "agents-md block missing from deploy.md");
  return match[1];
}

test("b-14: doc-check accepts the cutover docs and rejects a missing block or a retired word", async (t) => {
  const root = commitOperationalMirror(t, {
    "README.md": readFileSync(join(PROJECT_ROOT, "README.md")),
    "README.ja.md": readFileSync(join(PROJECT_ROOT, "README.ja.md")),
    "docs/deploy.md": readFileSync(join(PROJECT_ROOT, "docs", "deploy.md")),
  });
  const deployPath = join(root, "docs", "deploy.md");
  const original = readFileSync(deployPath, "utf8");
  const body = agentsMdBody(original);
  const transcript = join(root, "AGENTS.md");
  writeFileSync(transcript, `${body}\n`);
  const args = [
    "--transcript",
    `agents-md=${transcript}`,
    "--forbid",
    ...FORBID,
  ];

  const clean = await runDocCheck(root, args);
  assert.equal(clean.code, 0, clean.stderr);
  assert.ok(
    clean.stderr.includes("doc-check: 0 problems, 0 skipped"),
    clean.stderr,
  );

  const stripped = original
    .replace(/\r\n/g, "\n")
    .replace(
      /<!--\s*canonical:\s*agents-md\s*-->\s*\n```[\w]*\n[\s\S]*?\n```\n?/,
      "",
    );
  writeFileSync(deployPath, stripped);
  const missing = await runDocCheck(root, args);
  assert.equal(missing.code, 1, missing.stderr);
  assert.match(
    missing.stderr,
    /docs\/deploy\.md has 0 agents-md canonical blocks; exactly one is required/,
  );
  assert.ok(
    missing.stderr.includes("doc-check: 1 problem, 0 skipped"),
    missing.stderr,
  );

  writeFileSync(deployPath, `${original.replace(/\r\n/g, "\n")}\noutside to_tag here\n`);
  const outside = await runDocCheck(root, args);
  assert.equal(outside.code, 1, outside.stderr);
  assert.match(outside.stderr, /docs\/deploy\.md:\d+ contains to_tag/);
  assert.equal(
    outside.stderr.includes("doc-check: 0 problems, 0 skipped"),
    false,
  );

  const alone = commitOperationalMirror(t, {
    "README.md": "legacy_to_tag\n",
    "README.ja.md": "legacy_from_tag to_endpoints\n",
    "docs/deploy.md": `<!-- canonical: agents-md -->\n\`\`\`markdown\n${body}\n\`\`\`\nlegacy_to_tag\n`,
  });
  const aloneTranscript = join(alone, "AGENTS.md");
  writeFileSync(aloneTranscript, `${body}\n`);
  const bounded = await runDocCheck(alone, [
    "--transcript",
    `agents-md=${aloneTranscript}`,
    "--forbid",
    ...FORBID,
  ]);
  assert.equal(bounded.code, 0, bounded.stderr);
  assert.ok(
    bounded.stderr.includes("doc-check: 0 problems, 0 skipped"),
    bounded.stderr,
  );
  assert.equal(bounded.stderr.includes("contains to_tag"), false);
  assert.equal(bounded.stderr.includes("contains from_tag"), false);
  assert.equal(bounded.stderr.includes("contains to_endpoint"), false);
});