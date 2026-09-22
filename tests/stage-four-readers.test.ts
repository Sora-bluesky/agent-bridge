import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test, { type TestContext } from "node:test";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import Database from "better-sqlite3";
import {
  BridgeBus,
  type EndpointRow,
  PRESENTED_TTL_MS,
  createConsumerId,
  initializeBridgeDatabaseAtPath,
} from "../src/db.js";
import { BridgeTools, TOOL_DEFINITIONS } from "../src/tools.js";

const ROOT = resolve(fileURLToPath(new URL("..", import.meta.url)));
const SERVER = join(ROOT, "src", "server.ts");
const INIT = join(ROOT, "src", "bridge-init.ts");
const HOOK = join(ROOT, "src", "hook-notify.ts");
const SWEEP = join(ROOT, "src", "bridge-sweep.ts");
const T0 = Date.parse("2026-09-22T00:00:00.000Z");

function profile(t: TestContext): { userProfile: string; dbPath: string } {
  const userProfile = mkdtempSync(join(tmpdir(), "agent-bridge-e4b-"));
  const dbPath = join(userProfile, ".claude", "data", "agent-bridge", "bridge.db");
  t.after(() => rmSync(userProfile, { recursive: true, force: true }));
  initializeBridgeDatabaseAtPath(dbPath);
  return { userProfile, dbPath };
}

function withSql<T>(dbPath: string, fn: (db: Database.Database) => T): T {
  const db = new Database(dbPath);
  try {
    return fn(db);
  } finally {
    db.close();
  }
}

function tally(dbPath: string): { messages: number; deliveries: number; events: number } {
  return withSql(dbPath, (db) => ({
    messages: (db.prepare("SELECT COUNT(*) AS n FROM messages").get() as { n: number }).n,
    deliveries: (db.prepare("SELECT COUNT(*) AS n FROM deliveries").get() as { n: number }).n,
    events: (db.prepare("SELECT COUNT(*) AS n FROM events").get() as { n: number }).n,
  }));
}

function runProcess(
  entry: string,
  args: readonly string[],
  userProfile: string,
  stdin = "",
  extraEnv: Record<string, string> = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  const child = spawn(process.execPath, ["--import", "tsx", entry, ...args], {
    cwd: ROOT,
    env: { ...process.env, USERPROFILE: userProfile, AGENT_BRIDGE_TAG: "", ...extraEnv },
    stdio: ["pipe", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => { stdout += chunk; });
  child.stderr.on("data", (chunk: string) => { stderr += chunk; });
  child.stdin.end(stdin);
  return once(child, "close").then(([code]) => ({
    code: code as number | null,
    stdout,
    stderr,
  }));
}

function textOf(result: { content?: unknown }): string {
  const content = result.content;
  if (!Array.isArray(content) || content.length === 0) return "";
  const first = content[0] as { text?: string };
  return first.text ?? "";
}

async function openServer(role: "claude" | "codex", endpoint: string, userProfile: string) {
  let stderr = "";
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: ["--import", "tsx", SERVER, "--role", role, "--endpoint", endpoint],
    cwd: ROOT,
    env: { ...process.env, USERPROFILE: userProfile, AGENT_BRIDGE_TAG: "" },
    stderr: "pipe",
  });
  transport.stderr?.setEncoding("utf8");
  transport.stderr?.on("data", (chunk: string) => { stderr += chunk; });
  const client = new Client({ name: "stage-four-readers", version: "0" });
  await client.connect(transport);
  return {
    client,
    stderr: () => stderr,
    close: () => client.close(),
  };
}

async function tool(client: Client, name: string, args: Record<string, unknown>) {
  const result = await client.callTool({ name, arguments: args });
  return { isError: Boolean(result.isError), text: textOf(result) };
}

function jsonFrom(text: string): { messages: Array<{ message_id: string; attempt_id: string | null }> } {
  return JSON.parse(text.slice(text.indexOf("{"))) as {
    messages: Array<{ message_id: string; attempt_id: string | null }>;
  };
}

test("b-7: N=2 keeps deliveries independent and a second process cannot ack the holder", async (t) => {
  const { userProfile, dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  const a = bus.addEndpoint("codex", "a");
  const b = bus.addEndpoint("codex", "b");
  bus.addEndpoint("codex", "c");
  const messageId = randomUUID();
  const sent = bus.send({
    fromRole: "claude",
    toRole: "codex",
    subject: "n2",
    body: "hello",
    messageId,
    toEndpoints: ["a", "b"],
    sourceEndpoint: src,
    now: T0,
  });
  assert.equal(sent.idempotent, false);
  assert.deepEqual(sent.added, ["a", "b"]);
  bus.close();
  assert.equal(tally(dbPath).messages, 1);
  assert.equal(tally(dbPath).deliveries, 2);
  assert.equal(tally(dbPath).events, 2);

  const serverA = await openServer("codex", "a", userProfile);
  const serverB = await openServer("codex", "b", userProfile);
  try {
    const peekA = await tool(serverA.client, "bridge_fetch", { peek: true, limit: 10 });
    const peekB = await tool(serverB.client, "bridge_fetch", { peek: true, limit: 10 });
    assert.equal(jsonFrom(peekA.text).messages.length, 1);
    assert.equal(jsonFrom(peekB.text).messages.length, 1);
    const fetched = await tool(serverA.client, "bridge_fetch", { limit: 1 });
    assert.equal(fetched.isError, false, serverA.stderr());
    const body = jsonFrom(fetched.text);
    const attemptId = body.messages[0]?.attempt_id;
    assert.equal(body.messages[0]?.message_id, messageId);
    assert.equal(typeof attemptId, "string");
    const stolen = await tool(serverB.client, "bridge_ack", { message_id: messageId, attempt_id: attemptId });
    assert.equal(stolen.isError, true);
    assert.match(stolen.text, /not the holder/);
    const acked = await tool(serverA.client, "bridge_ack", { message_id: messageId, attempt_id: attemptId });
    assert.equal(acked.isError, false, acked.text);
  } finally {
    await serverA.close();
    await serverB.close();
  }
  const states = withSql(dbPath, (db) =>
    db.prepare(
      `SELECT ep.name AS name, d.state AS state
         FROM deliveries d JOIN endpoints ep ON ep.endpoint_id = d.endpoint_id
        WHERE d.message_id = ? ORDER BY ep.name`,
    ).all(messageId) as Array<{ name: string; state: string }>,
  );
  assert.deepEqual(states, [
    { name: "a", state: "confirmed" },
    { name: "b", state: "pending" },
  ]);

  const again = BridgeBus.open(dbPath);
  try {
    const same = again.send({
      fromRole: "claude", toRole: "codex", subject: "n2", body: "hello",
      messageId, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
    });
    assert.equal(same.idempotent, true);
    assert.deepEqual(same.added, []);
    const widened = again.send({
      fromRole: "claude", toRole: "codex", subject: "n2", body: "hello",
      messageId, toEndpoints: ["a", "c"], sourceEndpoint: src, now: T0,
    });
    assert.equal(widened.idempotent, true);
    assert.deepEqual(widened.added, ["c"]);
    const narrowed = again.send({
      fromRole: "claude", toRole: "codex", subject: "n2", body: "hello",
      messageId, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
    });
    assert.deepEqual(narrowed.added, []);
    assert.equal(tally(dbPath).deliveries, 3);
    const held = randomUUID();
    again.send({
      fromRole: "claude", toRole: "codex", subject: "hold", body: "hold",
      messageId: held, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
    });
  } finally {
    again.close();
  }

  const first = await openServer("codex", "a", userProfile);
  const second = await openServer("codex", "a", userProfile);
  try {
    const fetched = await tool(first.client, "bridge_fetch", { limit: 1 });
    const attemptId = jsonFrom(fetched.text).messages[0]?.attempt_id;
    const message = jsonFrom(fetched.text).messages[0]?.message_id;
    const denied = await tool(second.client, "bridge_ack", {
      message_id: message, attempt_id: attemptId,
    });
    assert.equal(denied.isError, true);
    assert.match(denied.text, /not the holder/);
    const allowed = await tool(first.client, "bridge_ack", {
      message_id: message, attempt_id: attemptId,
    });
    assert.equal(allowed.isError, false, allowed.text);
  } finally {
    await first.close();
    await second.close();
  }
  void a;
  void b;
});

test("b-8: startup refuses four distinct endpoint mistakes and bridge_hello is unknown", async (t) => {
  const { userProfile, dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  bus.addEndpoint("codex", "worker");
  bus.addEndpoint("claude", "gone");
  bus.close();
  withSql(dbPath, (db) => {
    db.prepare(`UPDATE endpoints SET retired_at = ? WHERE name = 'gone'`).run(new Date(T0).toISOString());
  });
  const absent = await runProcess(SERVER, ["--role", "claude"], userProfile);
  const unknown = await runProcess(SERVER, ["--role", "claude", "--endpoint", "nobody"], userProfile);
  const wrong = await runProcess(SERVER, ["--role", "claude", "--endpoint", "worker"], userProfile);
  const retired = await runProcess(SERVER, ["--role", "claude", "--endpoint", "gone"], userProfile);
  for (const row of [absent, unknown, wrong, retired]) {
    assert.notEqual(row.code, 0, row.stderr);
  }
  assert.match(absent.stderr, /missing --endpoint/);
  assert.match(unknown.stderr, /no endpoint named "nobody" is registered/);
  assert.match(wrong.stderr, /registered for codex, not claude/);
  assert.match(retired.stderr, /was retired at /);
  const texts = [absent.stderr, unknown.stderr, wrong.stderr, retired.stderr];
  assert.equal(new Set(texts).size, 4);
  assert.equal(TOOL_DEFINITIONS.some((tool) => tool.name === "bridge_hello"), false);
  const live = BridgeBus.open(dbPath);
  const src = live.addEndpoint("claude", "src");
  const tools = new BridgeTools(live, "claude", createConsumerId("claude"), { tag: null }, process.env, src);
  const hello = await tools.call("bridge_hello", { tag: "x" });
  assert.equal(hello.isError, true);
  assert.match(hello.content[0].text, /unknown tool: bridge_hello/);
  live.close();
});

test("b-9: sweep runs lease and presented recovery only and stuck counts every pending", async (t) => {
  const { userProfile, dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  const lane = bus.addEndpoint("codex", "lane");
  const sent = bus.send({
    fromRole: "claude", toRole: "codex", subject: "fresh", body: "body",
    toEndpoints: ["lane"], sourceEndpoint: src, now: T0,
  });
  const before = tally(dbPath).messages;
  const aged = withSql(dbPath, (db) => {
    db.prepare(
      `INSERT INTO messages (
         message_id, from_role, source_endpoint_id, legacy_to_tag,
         subject, body, envelope_sha256, envelope_version, body_sha256, sent_at
       ) VALUES (?, 'claude', ?, 'old-lane', 'aged', 'body', 'aa', 2, 'bb', ?)`,
    ).run(randomUUID(), src.endpoint_id, new Date(T0).toISOString());
    const id = randomUUID();
    db.prepare(
      `INSERT INTO messages (
         message_id, from_role, source_endpoint_id, subject, body,
         envelope_sha256, envelope_version, body_sha256, sent_at
       ) VALUES (?, 'claude', ?, 'lease', 'body', 'aa', 2, 'bb', ?)`,
    ).run(id, src.endpoint_id, new Date(T0 - 60_000).toISOString());
    db.prepare(
      `INSERT INTO deliveries (
         message_id, endpoint_id, state, holder, attempt_id, attempt_count, lease_until
       ) VALUES (?, ?, 'leased', 'holder', ?, 1, ?)`,
    ).run(id, lane.endpoint_id, randomUUID(), T0 - 1);
    return id;
  });
  const first = bus.recover("codex", T0);
  assert.equal(first.leaseExpired, 1);
  assert.equal(first.requeued, 0);
  assert.equal(first.bounced, 0);
  assert.equal(first.fallbackDemoted, 0);
  assert.equal(tally(dbPath).messages, before + 2);
  const backlog = bus.backlog("codex");
  assert.ok(backlog.stuck >= 2);
  assert.equal(backlog.oldestSentAt, new Date(T0 - 60_000).toISOString());
  withSql(dbPath, (db) => {
    db.prepare(
      `UPDATE deliveries
          SET state = 'presented', holder = 'holder', attempt_id = ?,
              lease_until = NULL, presented_at = ?
        WHERE message_id = ?`,
    ).run(randomUUID(), new Date(T0 - PRESENTED_TTL_MS - 1000).toISOString(), aged);
  });
  const second = bus.recover("codex", T0);
  assert.equal(second.requeued, 1);
  assert.equal(tally(dbPath).messages, before + 2);
  bus.close();
  const sweep = await runProcess(SWEEP, [], userProfile);
  assert.equal(sweep.code, 0, sweep.stderr);
  assert.match(sweep.stderr, /claude=lease:\d+,requeued:\d+,stuck:\d+,oldest:[^ ]+ codex=lease:\d+,requeued:\d+,stuck:\d+,oldest:/);
  assert.doesNotMatch(sweep.stderr, /bounced:|fallback:/);
  void sent;
});

test("b-10: --cancel ends pending deliveries and refuses held or unknown targets", async (t) => {
  const { userProfile, dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  const a = bus.addEndpoint("codex", "a");
  const b = bus.addEndpoint("codex", "b");
  const messageId = randomUUID();
  bus.send({
    fromRole: "claude", toRole: "codex", subject: "cancel", body: "body",
    messageId, toEndpoints: ["a", "b"], sourceEndpoint: src, now: T0,
  });
  assert.equal(bus.backlog("codex").stuck, 2);
  bus.close();
  const cancelled = await runProcess(INIT, ["--cancel", messageId, "--reason", "operator stop"], userProfile);
  assert.equal(cancelled.code, 0, cancelled.stderr);
  assert.match(cancelled.stderr, /cancelled message_id=/);
  const after = withSql(dbPath, (db) => ({
    states: db.prepare(`SELECT state FROM deliveries WHERE message_id = ?`).all(messageId) as Array<{ state: string }>,
    details: db.prepare(
      `SELECT e.detail AS detail FROM events e JOIN deliveries d ON d.delivery_id = e.delivery_id
        WHERE d.message_id = ? AND e.event = 'cancelled'`,
    ).all(messageId) as Array<{ detail: string }>,
  }));
  assert.deepEqual(after.states.map((row) => row.state).sort(), ["cancelled", "cancelled"]);
  assert.equal(after.details.length, 2);
  assert.match(after.details[0].detail, /operator stop/);
  const reader = BridgeBus.open(dbPath);
  const page = reader.fetch("codex", createConsumerId("codex"), { peek: true, limit: 10, endpoint: a });
  assert.equal(page.messages.length, 0);
  assert.equal(reader.backlog("codex").stuck, 0);
  const leasedId = randomUUID();
  reader.send({
    fromRole: "claude", toRole: "codex", subject: "leased", body: "body",
    messageId: leasedId, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
  });
  reader.claim("codex", createConsumerId("codex"), 1, T0, null, a);
  reader.close();
  const eventsBefore = tally(dbPath).events;
  const refused = await runProcess(INIT, ["--cancel", leasedId, "--endpoint", "a", "--reason", "no"], userProfile);
  assert.notEqual(refused.code, 0);
  assert.match(refused.stderr, /is leased/);
  assert.equal(tally(dbPath).events, eventsBefore);
  const presentedId = randomUUID();
  const presenter = BridgeBus.open(dbPath);
  presenter.send({
    fromRole: "claude", toRole: "codex", subject: "shown", body: "body",
    messageId: presentedId, toEndpoints: ["b"], sourceEndpoint: src, now: T0,
  });
  presenter.fetch("codex", createConsumerId("codex"), { limit: 1, endpoint: b, now: T0 });
  presenter.close();
  const presented = await runProcess(INIT, ["--cancel", presentedId, "--endpoint", "b", "--reason", "no"], userProfile);
  assert.match(presented.stderr, /is presented/);
  const missing = await runProcess(INIT, ["--cancel", randomUUID(), "--reason", "no"], userProfile);
  assert.match(missing.stderr, /message_id not found/);
  const missingEndpoint = await runProcess(INIT, ["--cancel", presentedId, "--endpoint", "nope", "--reason", "no"], userProfile);
  assert.match(missingEndpoint.stderr, /no endpoint named "nope"/);
  void b;
});

test("b-10b: --retire-endpoint refuses while a delivery is pending, leased or presented, and retires once they are confirmed", (t) => {
  const { dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  const a = bus.addEndpoint("codex", "a");
  const consumer = createConsumerId("codex");
  const leasedId = randomUUID();
  bus.send({
    fromRole: "claude", toRole: "codex", subject: "lease", body: "body",
    messageId: leasedId, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
  });
  assert.throws(() => bus.retireEndpoint("codex", "a"), /refusing retirement/);
  const claimed = bus.claim("codex", consumer, 1, T0, null, a);
  assert.equal(claimed.length, 1);
  assert.throws(() => bus.retireEndpoint("codex", "a"), /refusing retirement/);
  // presented: an expired presentation is requeued by the sweep, so it
  // holds retirement as well; confirmed is terminal and releases it.
  const shownId = randomUUID();
  bus.send({
    fromRole: "claude", toRole: "codex", subject: "show", body: "body",
    messageId: shownId, toEndpoints: ["a"], sourceEndpoint: src, now: T0,
  });
  const page = bus.fetch("codex", consumer, { limit: 1, endpoint: a, now: T0 });
  assert.equal(page.messages.length, 1);
  // The leased row is settled in place (the shape the CHECK allows for
  // confirmed), since ack is for presented deliveries only.
  withSql(dbPath, (db) => {
    db.prepare(
      `UPDATE deliveries SET state = 'confirmed', lease_until = NULL, presented_at = ?, confirmed_at = ?
        WHERE message_id = ?`,
    ).run(new Date(T0).toISOString(), new Date(T0).toISOString(), leasedId);
  });
  assert.throws(() => bus.retireEndpoint("codex", "a"), /refusing retirement/);
  bus.ack("codex", shownId, page.messages[0].attempt_id, T0, consumer, a);
  const retired = bus.retireEndpoint("codex", "a");
  assert.notEqual(retired.retired_at, null);
  bus.close();
});

test("b-11: removed arguments and bad destinations write no message, delivery, or event", async (t) => {
  const { dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  const a = bus.addEndpoint("codex", "a");
  bus.addEndpoint("codex", "old");
  bus.retireEndpoint("codex", "old");
  const tools = new BridgeTools(bus, "claude", createConsumerId("claude"), { tag: null }, process.env, src);
  const before = tally(dbPath);
  const good = await tools.call("bridge_send", {
    subject: "ok", body: "body", to_endpoints: ["a"],
  });
  assert.ok(!good.isError, good.content[0].text);
  const written = tally(dbPath);
  assert.equal(written.messages, before.messages + 1);
  assert.equal(written.deliveries, before.deliveries + 1);
  assert.equal(written.events, before.events + 1);
  const messageId = good.content[0].text.match(/bridge 送信: ([0-9a-f-]+)/)?.[1];
  assert.ok(messageId);
  for (const extra of [
    { to_tag: "lane" },
    { broadcast: true },
    { on_timeout: "bounce" },
    { to_endpoint: "a" },
  ]) {
    const mark = tally(dbPath);
    const refused = await tools.call("bridge_send", {
      subject: "no", body: "body", to_endpoints: ["a"], ...extra,
    });
    assert.equal(refused.isError, true);
    assert.match(refused.content[0].text, new RegExp(Object.keys(extra)[0]));
    assert.deepEqual(tally(dbPath), mark);
  }
  for (const [args, pattern] of [
    [{ subject: "e", body: "body", to_endpoints: [] }, /at least one/],
    [{ subject: "d", body: "body", to_endpoints: ["a", "a"] }, /repeats a/],
    [{ subject: "s", body: "body", to_endpoints: ["src"] }, /not codex/],
    [{ subject: "r", body: "body", to_endpoints: ["old"] }, /retired/],
    [{ subject: "u", body: "body", to_endpoints: ["missing"] }, /no endpoint named/],
  ] as const) {
    const mark = tally(dbPath);
    const refused = await tools.call("bridge_send", args);
    assert.equal(refused.isError, true, JSON.stringify(args));
    assert.match(refused.content[0].text, pattern);
    assert.deepEqual(tally(dbPath), mark);
  }
  const mark = tally(dbPath);
  const mixed = await tools.call("bridge_send", {
    subject: "ok", body: "body", message_id: messageId, to_endpoints: ["a", "missing"],
  });
  assert.equal(mixed.isError, true);
  assert.match(mixed.content[0].text, /no endpoint named/);
  assert.deepEqual(tally(dbPath), mark);
  bus.close();
  void a;
});

test("b-11b: a send from an endpoint retired after startup is refused and writes nothing", (t) => {
  const { dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  bus.addEndpoint("codex", "a");
  bus.retireEndpoint("claude", "src");
  const before = tally(dbPath);
  assert.throws(
    () => bus.send({
      fromRole: "claude", toRole: "codex", subject: "late", body: "body",
      messageId: randomUUID(), toEndpoints: ["a"], sourceEndpoint: src, now: T0,
    }),
    /was retired at/,
  );
  assert.deepEqual(tally(dbPath), before);
  bus.close();
});

test("b-12: hook buckets match the nine-row reader table and stay silent without an endpoint", async (t) => {
  const { userProfile, dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("codex", "src");
  const here = bus.addEndpoint("claude", "here");
  const other = bus.addEndpoint("claude", "other");
  // The hook resolves its name in the Claude role; a Codex endpoint that
  // shares the name (allowed by UNIQUE (role, name)) must not blank it.
  bus.addEndpoint("codex", "here");
  bus.close();
  const stamp = new Date(T0).toISOString();
  withSql(dbPath, (db) => {
    const add = (
      endpoint: EndpointRow,
      state: string,
      fields: { holder?: string; attempt?: string; lease?: number | null; presented?: string | null } = {},
    ) => {
      const id = randomUUID();
      db.prepare(
        `INSERT INTO messages (
           message_id, from_role, source_endpoint_id, subject, body,
           envelope_sha256, envelope_version, body_sha256, sent_at
         ) VALUES (?, 'codex', ?, ?, 'body', 'aa', 2, 'bb', ?)`,
      ).run(id, src.endpoint_id, state, stamp);
      db.prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state, holder, attempt_id, attempt_count,
           lease_until, presented_at, confirmed_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
      ).run(
        id,
        endpoint.endpoint_id,
        state,
        fields.holder ?? null,
        fields.attempt ?? null,
        fields.attempt ? 1 : 0,
        fields.lease ?? null,
        fields.presented ?? null,
      );
    };
    add(here, "pending");
    add(here, "pending");
    add(here, "pending");
    /*
     * The live rows are live for the hook process too, which reads the
     * real clock; the expired rows are expired for both clocks.
     */
    add(here, "leased", { holder: "live", attempt: randomUUID(), lease: Date.now() + 60_000 });
    add(here, "leased", { holder: "dead", attempt: randomUUID(), lease: T0 - 1 });
    add(here, "bounced");
    add(here, "presented", { holder: "live", attempt: randomUUID(), presented: new Date().toISOString() });
    add(here, "presented", {
      holder: "stale",
      attempt: randomUUID(),
      presented: new Date(T0 - PRESENTED_TTL_MS - 1000).toISOString(),
    });
    add(other, "pending");
  });
  const { countPendingClaudeMessages } = await import("../src/hook-notify.js");
  const counts = countPendingClaudeMessages(dbPath, T0, "here");
  assert.equal(counts.pending_here, 3);
  assert.equal(counts.expired_leased, 1);
  assert.equal(counts.expired_presented, 1);
  assert.equal(counts.pending_elsewhere, 1);
  assert.equal(counts.fetchable, 5);
  assert.equal(counts.total, 5);
  const hooked = await runProcess(
    HOOK, ["--event", "user-prompt-submit"], userProfile, "{}",
    { AGENT_BRIDGE_ENDPOINT: "here" },
  );
  assert.equal(hooked.code, 0, hooked.stderr);
  const parsed = JSON.parse(hooked.stdout) as { hookSpecificOutput: { additionalContext: string } };
  const notice = parsed.hookSpecificOutput.additionalContext;
  assert.match(notice, /pending_here=3/);
  assert.match(notice, /取得可能=5/);
  assert.doesNotMatch(notice, /bridge_hello|to_tag|untagged|expired tag|期限切れ ?tag/);
  const silent = await runProcess(
    HOOK, ["--event", "user-prompt-submit"], userProfile, "{}",
    { AGENT_BRIDGE_ENDPOINT: "" },
  );
  assert.equal(silent.code, 0, silent.stderr);
  assert.equal(silent.stdout, "");
});

test("b-13: bridge_status returns deliveries and counts them, with no top-level status", async (t) => {
  const { dbPath } = profile(t);
  const bus = BridgeBus.open(dbPath);
  const src = bus.addEndpoint("claude", "src");
  bus.addEndpoint("codex", "a");
  bus.addEndpoint("codex", "b");
  const messageId = randomUUID();
  bus.send({
    fromRole: "claude", toRole: "codex", subject: "status", body: "body",
    messageId, toEndpoints: ["a", "b"], sourceEndpoint: src, now: T0,
  });
  const status = bus.status(messageId);
  assert.equal("status" in status, false);
  assert.equal(status.message, undefined);
  assert.equal(status.deliveries?.length, 2);
  assert.deepEqual(
    status.deliveries?.map((row) => row.endpoint).sort(),
    ["a", "b"],
  );
  assert.equal(status.deliveries?.every((row) => row.state === "pending"), true);
  assert.equal(status.unacked_total, 2);
  assert.equal(status.recovery_owed, 0);
  assert.equal(status.event_counts.sent, 2);
  assert.equal(status.events.length, 2);
  bus.fetch("codex", createConsumerId("codex"), {
    endpoint: bus.resolveEndpoint("codex", "a"), limit: 1, now: T0,
  });
  const later = bus.status(messageId);
  assert.equal(later.unacked_total, 2);
  assert.equal(later.deliveries?.filter((row) => row.state === "pending").length, 1);
  assert.equal(later.deliveries?.filter((row) => row.state === "presented").length, 1);
  bus.close();
});