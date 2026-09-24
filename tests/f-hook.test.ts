import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import {
  BridgeBus,
  type EndpointMapping,
  type EndpointRow,
  createConsumerId,
  initializeBridgeDatabaseAtPath,
  migrateBridgeDatabaseAtPath,
} from "../src/db.js";
import { schemaOlderThanObligations } from "../src/hook-notify.js";

const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const HOOK = join(ROOT, "src", "hook-notify.ts");
const T0 = Date.parse("2026-09-22T00:00:00.000Z");
const STAMP = "2026-09-22T00:00:00.000Z";
const UNMEASURABLE = "このDBでは義務を測れません。";
const UNREADABLE = "義務の件数は読めませんでした。";
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

interface Lane {
  userProfile: string;
  dbPath: string;
  bus: BridgeBus;
  src: EndpointRow;
  lane: EndpointRow;
}

function userProfileOf(t: TestContext): string {
  const userProfile = mkdtempSync(join(tmpdir(), "agent-bridge-fhook-"));
  t.after(() => rmSync(userProfile, { recursive: true, force: true }));
  return userProfile;
}

function dbPathOf(userProfile: string): string {
  return join(userProfile, ".claude", "data", "agent-bridge", "bridge.db");
}

function withDb<T>(path: string, work: (db: Database.Database) => T): T {
  const db = new Database(path);
  try {
    return work(db);
  } finally {
    db.close();
  }
}

function readMeta(path: string, key: string): string | null {
  const db = new Database(path, { readonly: true, fileMustExist: true });
  try {
    const row = db.prepare("SELECT v FROM meta WHERE k = ?").get(key) as
      | { v: string }
      | undefined;
    return row?.v ?? null;
  } finally {
    db.close();
  }
}

function runHook(
  userProfile: string,
  event: "stop" | "user-prompt-submit",
  endpoint: string,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  const child = spawn(
    process.execPath,
    ["--import", "tsx", HOOK, "--event", event],
    {
      cwd: ROOT,
      env: {
        ...process.env,
        USERPROFILE: userProfile,
        AGENT_BRIDGE_TAG: "",
        AGENT_BRIDGE_ENDPOINT: endpoint,
      },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk: string) => {
    stderr += chunk;
  });
  child.stdin.end("{}");
  return once(child, "close").then(([code]) => ({
    code: code as number | null,
    stdout,
    stderr,
  }));
}

function noticeOf(stdout: string): string {
  const parsed = JSON.parse(stdout) as {
    reason?: string;
    hookSpecificOutput?: { additionalContext?: string };
  };
  return parsed.hookSpecificOutput?.additionalContext ?? parsed.reason ?? "";
}

function zeroStyled(text: string): boolean {
  return text.includes("owed=0") || text.includes("awaiting=0");
}

function openLane(t: TestContext): Lane {
  const userProfile = userProfileOf(t);
  const dbPath = dbPathOf(userProfile);
  initializeBridgeDatabaseAtPath(dbPath);
  const bus = BridgeBus.open(dbPath);
  t.after(() => bus.close());
  return {
    userProfile,
    dbPath,
    bus,
    src: bus.addEndpoint("codex", "src", new Date(T0)),
    lane: bus.addEndpoint("claude", "lane", new Date(T0)),
  };
}

function sendToLane(desk: Lane, expectsReply: boolean): string {
  const messageId = randomUUID();
  desk.bus.send({
    fromRole: "codex",
    toRole: "claude",
    subject: expectsReply ? "need" : "note",
    body: "body",
    messageId,
    toEndpoints: ["lane"],
    sourceEndpoint: desk.src,
    expectsReply,
    now: T0,
  });
  return messageId;
}

function acknowledge(desk: Lane, messageId: string): void {
  const consumer = createConsumerId("claude");
  const fetched = desk.bus.fetch("claude", consumer, {
    messageId,
    endpoint: desk.lane,
    now: T0,
  });
  const attemptId = fetched.messages[0]?.attempt_id;
  assert.equal(typeof attemptId, "string");
  desk.bus.ack("claude", messageId, attemptId, T0, consumer, desk.lane);
}

function at413(t: TestContext): { userProfile: string; dbPath: string } {
  const userProfile = userProfileOf(t);
  const dbPath = dbPathOf(userProfile);
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
    stopAt: "4.13",
    skipCutoverChecks: true,
  });
  return { userProfile, dbPath };
}

function insertPending413(dbPath: string): void {
  withDb(dbPath, (db) => {
    const source = (
      db
        .prepare(
          "SELECT endpoint_id FROM endpoints WHERE role = 'codex' AND name = 'codex-main'",
        )
        .get() as { endpoint_id: string }
    ).endpoint_id;
    const dest = (
      db
        .prepare(
          "SELECT endpoint_id FROM endpoints WHERE role = 'claude' AND name = 'claude-main'",
        )
        .get() as { endpoint_id: string }
    ).endpoint_id;
    const messageId = randomUUID();
    db.prepare(
      `INSERT INTO messages (
         message_id, from_role, source_endpoint_id, subject, body,
         envelope_sha256, envelope_version, body_sha256, sent_at
       ) VALUES (?, 'codex', ?, 'old', 'body', 'aa', 2, 'bb', ?)`,
    ).run(messageId, source, STAMP);
    db.prepare(
      "INSERT INTO deliveries (message_id, endpoint_id, state) VALUES (?, ?, 'pending')",
    ).run(messageId, dest);
  });
}

function sealCopy(dbPath: string, version: string): void {
  withDb(dbPath, (db) => {
    db.pragma("wal_checkpoint(TRUNCATE)");
  });
  rmSync(`${dbPath}-wal`, { force: true });
  rmSync(`${dbPath}-shm`, { force: true });
  const copy = `${dbPath}.schema-copy`;
  copyFileSync(dbPath, copy);
  withDb(copy, (db) => {
    const changed = db
      .prepare("UPDATE meta SET v = ? WHERE k = 'schema_version'")
      .run(version);
    assert.equal(changed.changes, 1);
  });
  copyFileSync(copy, dbPath);
  rmSync(`${dbPath}-wal`, { force: true });
  rmSync(`${dbPath}-shm`, { force: true });
}

test("f-17a: owed without fetchable notifies and does not block Stop", async (t) => {
  const desk = openLane(t);
  acknowledge(desk, sendToLane(desk, true));
  desk.bus.close();
  const prompt = await runHook(desk.userProfile, "user-prompt-submit", "lane");
  assert.equal(prompt.code, 0, prompt.stderr);
  assert.equal(prompt.stderr.includes("hook skipped"), false, prompt.stderr);
  const notice = noticeOf(prompt.stdout);
  assert.match(notice, /取得可能=0（/);
  assert.match(notice, /owed=1/);
  assert.match(notice, /awaiting=0/);
  const stop = await runHook(desk.userProfile, "stop", "lane");
  assert.equal(stop.code, 0, stop.stderr);
  assert.equal(stop.stdout, "");
  assert.equal(stop.stderr.includes("hook skipped"), false, stop.stderr);
});

test("f-17b: fetchable mail still blocks Stop", async (t) => {
  const desk = openLane(t);
  sendToLane(desk, false);
  desk.bus.close();
  const stop = await runHook(desk.userProfile, "stop", "lane");
  assert.equal(stop.code, 0, stop.stderr);
  const parsed = JSON.parse(stop.stdout) as { decision?: string; reason?: string };
  assert.equal(parsed.decision, "block");
  assert.match(parsed.reason ?? "", /取得可能=[1-9]/);
});

test("f-17c: a 4.13 database with pending mail cannot measure obligations", async (t) => {
  const made = at413(t);
  assert.equal(readMeta(made.dbPath, "schema_version"), "4.13");
  insertPending413(made.dbPath);
  const prompt = await runHook(made.userProfile, "user-prompt-submit", "claude-main");
  assert.equal(prompt.code, 0, prompt.stderr);
  assert.equal(prompt.stderr.includes("hook skipped"), false, prompt.stderr);
  const notice = noticeOf(prompt.stdout);
  assert.match(notice, /取得可能=[1-9]/);
  assert.ok(notice.includes(UNMEASURABLE), notice);
  assert.equal(notice.includes(UNREADABLE), false);
  assert.equal(zeroStyled(notice), false, notice);
});

test("f-17d: schema_version 4.9 is older than 4.14", async (t) => {
  assert.equal("4.9" > "4.14", true);
  assert.equal(schemaOlderThanObligations("4.9"), true);
  assert.equal(schemaOlderThanObligations("4.13"), true);
  assert.equal(schemaOlderThanObligations("4.14"), false);
  const desk = openLane(t);
  sendToLane(desk, false);
  desk.bus.close();
  sealCopy(desk.dbPath, "4.9");
  assert.equal(readMeta(desk.dbPath, "schema_version"), "4.9");
  const prompt = await runHook(desk.userProfile, "user-prompt-submit", "lane");
  assert.equal(prompt.code, 0, prompt.stderr);
  assert.equal(prompt.stderr.includes("hook skipped"), false, prompt.stderr);
  const notice = noticeOf(prompt.stdout);
  assert.ok(notice.includes(UNMEASURABLE), notice);
  assert.equal(zeroStyled(notice), false, notice);
});

test("f-17e: a failed obligation query still yields the pending notice", async (t) => {
  const desk = openLane(t);
  sendToLane(desk, false);
  desk.bus.close();
  withDb(desk.dbPath, (db) => {
    db.exec("ALTER TABLE messages RENAME COLUMN expects_reply TO expects_reply_gone");
  });
  const prompt = await runHook(desk.userProfile, "user-prompt-submit", "lane");
  assert.equal(prompt.code, 0, prompt.stderr);
  assert.equal(prompt.stderr.includes("hook skipped"), false, prompt.stderr);
  const notice = noticeOf(prompt.stdout);
  assert.match(notice, /取得可能=[1-9]/);
  assert.ok(notice.includes(UNREADABLE), notice);
  assert.equal(notice.includes(UNMEASURABLE), false);
  assert.equal(zeroStyled(notice), false, notice);
});