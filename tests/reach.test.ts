import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import Database from "better-sqlite3";
import {
  BridgeBus,
  CLAIM_LEASE_MS,
  type EndpointRow,
  type FetchMessage,
  type FetchResult,
  createConsumerId,
  initializeBridgeDatabaseAtPath,
} from "../src/db.js";
import { createHookOutput } from "../src/hook-notify.js";

const T0 = Date.parse("2026-09-22T00:00:00.000Z");
const ROLE = "codex" as const;
const PAGE = 10;
const ROUNDS = 5;

interface Inbox {
  dbPath: string;
  bus: BridgeBus;
  dest: EndpointRow;
  consumer: string;
  ids: string[];
}

function withDb<T>(
  dbPath: string,
  work: (db: Database.Database) => T,
): T {
  const db = new Database(dbPath, { fileMustExist: true });
  try {
    return work(db);
  } finally {
    db.close();
  }
}

function openInbox(t: TestContext, count: number): Inbox {
  const userProfile = mkdtempSync(join(tmpdir(), "agent-bridge-reach-"));
  const dbPath = join(userProfile, ".claude", "data", "agent-bridge", "bridge.db");
  initializeBridgeDatabaseAtPath(dbPath);
  const bus = BridgeBus.open(dbPath);
  t.after(() => {
    /* Close before removing: on Windows an open database file refuses rm. */
    bus.close();
    rmSync(userProfile, { recursive: true, force: true });
  });
  const src = bus.addEndpoint("claude", "src", new Date(T0));
  const dest = bus.addEndpoint(ROLE, "lane", new Date(T0));
  const ids: string[] = [];
  for (let i = 0; i < count; i += 1) {
    const messageId = randomUUID();
    bus.send({
      fromRole: "claude",
      toRole: ROLE,
      subject: `m${i}`,
      body: `body-${i}`,
      messageId,
      toEndpoints: ["lane"],
      sourceEndpoint: src,
      now: T0,
    });
    ids.push(messageId);
  }
  return { dbPath, bus, dest, consumer: createConsumerId(ROLE), ids };
}

function countState(dbPath: string, endpointId: string, state: string): number {
  return withDb(dbPath, (db) => {
    const row = db
      .prepare(
        `SELECT COUNT(*) AS n
           FROM deliveries
          WHERE endpoint_id = ? AND state = ?`,
      )
      .get(endpointId, state) as { n: number };
    return row.n;
  });
}

function deliveryIdOf(dbPath: string, messageId: string): number {
  return withDb(dbPath, (db) => {
    const row = db
      .prepare("SELECT delivery_id AS id FROM deliveries WHERE message_id = ?")
      .get(messageId) as { id: number };
    return row.id;
  });
}

function peekTurn(inbox: Inbox, now: number, maxRounds = ROUNDS): FetchResult[] {
  const pages: FetchResult[] = [];
  let cursor: number | undefined;
  for (let round = 0; round < maxRounds; round += 1) {
    const page = inbox.bus.fetch(ROLE, inbox.consumer, {
      peek: true,
      limit: PAGE,
      cursor,
      endpoint: inbox.dest,
      now,
    });
    pages.push(page);
    if (!page.has_more) break;
    assert.equal(typeof page.next_cursor, "number");
    cursor = page.next_cursor ?? undefined;
  }
  return pages;
}

function listedIds(pages: readonly FetchResult[]): string[] {
  return pages.flatMap((page) => page.messages.map((row) => row.message_id));
}

function takeById(inbox: Inbox, messageIds: readonly string[], now: number): void {
  for (const messageId of messageIds) {
    const fetched = inbox.bus.fetch(ROLE, inbox.consumer, {
      messageId,
      endpoint: inbox.dest,
      now,
    });
    assert.equal(fetched.messages.length, 1);
    const row = fetched.messages[0];
    assert.ok(row);
    assert.equal(row.message_id, messageId);
    assert.equal(typeof row.attempt_id, "string");
    inbox.bus.ack(ROLE, messageId, row.attempt_id, now, inbox.consumer, inbox.dest);
  }
}

function assertTenWithMore(pages: readonly FetchResult[]): void {
  assert.equal(pages.length, ROUNDS);
  for (const page of pages) {
    assert.equal(page.messages.length, PAGE);
    assert.equal(page.has_more, true);
  }
}

function assertBodies(rows: readonly FetchMessage[], ids: readonly string[]): void {
  assert.equal(rows.length, ids.length);
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    assert.ok(row);
    assert.equal(row.message_id, ids[index]);
    assert.equal(row.body, `body-${index}`);
    assert.equal(typeof row.attempt_id, "string");
  }
}

test("reach-1: three turn-head turns drain 120 pending deliveries in id order", (t) => {
  const inbox = openInbox(t, 120);
  const turn1 = peekTurn(inbox, T0);
  assertTenWithMore(turn1);
  assert.deepEqual(listedIds(turn1), inbox.ids.slice(0, 50));
  takeById(inbox, listedIds(turn1), T0);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 70);

  const turn2 = peekTurn(inbox, T0);
  assertTenWithMore(turn2);
  assert.deepEqual(listedIds(turn2), inbox.ids.slice(50, 100));
  takeById(inbox, listedIds(turn2), T0);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 20);

  const turn3 = peekTurn(inbox, T0);
  assert.equal(turn3.length, 2);
  assert.equal(turn3[0]?.messages.length, PAGE);
  assert.equal(turn3[0]?.has_more, true);
  assert.equal(turn3[1]?.messages.length, PAGE);
  assert.equal(turn3[1]?.has_more, false);
  assert.deepEqual(listedIds(turn3), inbox.ids.slice(100));
  takeById(inbox, listedIds(turn3), T0);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 0);
});

test("reach-1: an expired unacked lease keeps its delivery_id and leads the next turn", (t) => {
  const inbox = openInbox(t, 120);
  const turn1 = peekTurn(inbox, T0);
  assertTenWithMore(turn1);
  const listed = listedIds(turn1);
  const held = listed[0];
  assert.ok(held);
  assert.equal(held, inbox.ids[0]);
  const before = deliveryIdOf(inbox.dbPath, held);
  // fetch() presents before returning, so the lease left unacked is claim().
  const claimed = inbox.bus.claim(ROLE, inbox.consumer, 1, T0, null, inbox.dest);
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0]?.message_id, held);
  takeById(inbox, listed.slice(1), T0);
  // claim sets lease_until to now + CLAIM_LEASE_MS; recovery uses lease_until < now.
  const expiredAt = T0 + CLAIM_LEASE_MS + 1;
  const recovered = inbox.bus.recover(ROLE, expiredAt);
  assert.equal(recovered.leaseExpired, 1);
  assert.equal(recovered.requeued, 0);
  assert.equal(deliveryIdOf(inbox.dbPath, held), before);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 71);
  const next = inbox.bus.fetch(ROLE, inbox.consumer, {
    peek: true,
    limit: PAGE,
    endpoint: inbox.dest,
    now: expiredAt,
  });
  assert.equal(next.messages.length, PAGE);
  assert.equal(next.has_more, true);
  assert.equal(next.messages[0]?.message_id, held);
});

test("reach-2: five peeks then five limit-10 claims confirm 50 and every claim row has a body", (t) => {
  const inbox = openInbox(t, 120);
  const peeked = peekTurn(inbox, T0);
  assertTenWithMore(peeked);
  const seen = listedIds(peeked);
  assert.deepEqual(seen, inbox.ids.slice(0, 50));
  const claimed: FetchMessage[] = [];
  for (let round = 0; round < ROUNDS; round += 1) {
    const page = inbox.bus.fetch(ROLE, inbox.consumer, {
      peek: false,
      limit: PAGE,
      endpoint: inbox.dest,
      now: T0,
    });
    assert.equal(page.peek, false);
    assert.equal(page.messages.length, PAGE);
    claimed.push(...page.messages);
  }
  assertBodies(claimed, seen);
  for (const row of claimed) {
    inbox.bus.ack(ROLE, row.message_id, row.attempt_id, T0, inbox.consumer, inbox.dest);
  }
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "confirmed"), 50);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 70);
});

test("reach-2: fetch(peek=false, limit=10) with no prior peek still claims 10 and returns bodies", (t) => {
  const inbox = openInbox(t, 12);
  const page = inbox.bus.fetch(ROLE, inbox.consumer, {
    peek: false,
    limit: PAGE,
    endpoint: inbox.dest,
    now: T0,
  });
  assert.equal(page.peek, false);
  assert.equal(page.messages.length, PAGE);
  assertBodies(page.messages, inbox.ids.slice(0, PAGE));
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "presented"), PAGE);
  assert.equal(countState(inbox.dbPath, inbox.dest.endpoint_id, "pending"), 2);
});

test("reach-3: the hook notice and the canonical rule both allow the bulk claim after a peek", () => {
  const output = createHookOutput("user-prompt-submit", {
    pending_here: 1,
    expired_leased: 0,
    expired_presented: 0,
    pending_elsewhere: 0,
    fetchable: 1,
    total: 1,
    endpoint: "lane",
    role: "claude",
  });
  assert.ok(output);
  const notice = (
    JSON.parse(output) as {
      hookSpecificOutput: { additionalContext: string };
    }
  ).hookSpecificOutput.additionalContext;
  const deploy = readFileSync(
    new URL("../docs/deploy.md", import.meta.url),
    "utf8",
  ).replace(/\r\n/g, "\n");
  const canonical = deploy.match(
    /<!--\s*canonical:\s*agents-md\s*-->\s*\n```[\w]*\n([\s\S]*?)\n```/,
  )?.[1];
  assert.ok(canonical, "agents-md block missing from deploy.md");
  // The rule reaches Claude through the hook notice and Codex through
  // AGENTS.md. Both copies have to say the same thing about taking mail.
  for (const text of [notice, canonical]) {
    assert.ok(text.includes("bridge_fetch(message_id=<"), text);
    assert.ok(text.includes("bridge_fetch(limit=10)"), text);
    for (const word of [
      "expects_reply",
      "in_reply_to",
      "reply_kind",
      "owed",
      "awaiting",
    ]) {
      assert.equal(
        new RegExp(`(?:^|[^A-Za-z0-9_])${word}(?:[^A-Za-z0-9_]|$)`).test(text),
        true,
        `${word} missing`,
      );
    }
  }
  assert.equal(notice.includes("取る便はbridge_fetch(message_id="), false);
});
