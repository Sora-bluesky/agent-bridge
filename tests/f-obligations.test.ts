import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import Database from "better-sqlite3";
import {
  BridgeBus,
  type EndpointRow,
  type FetchResult,
  createConsumerId,
  initializeBridgeDatabaseAtPath,
} from "../src/db.js";
import { BridgeTools } from "../src/tools.js";

const T0 = Date.parse("2026-09-22T00:00:00.000Z");

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

const SENT = iso(T0);

interface Desk {
  dbPath: string;
  bus: BridgeBus;
  src: EndpointRow;
  a: EndpointRow;
  b: EndpointRow;
  c: EndpointRow;
  claudeConsumer: string;
  codexConsumer: string;
}

function openDesk(t: TestContext): Desk {
  const root = mkdtempSync(join(tmpdir(), "agent-bridge-f-"));
  const dbPath = join(root, ".claude", "data", "agent-bridge", "bridge.db");
  initializeBridgeDatabaseAtPath(dbPath);
  const bus = BridgeBus.open(dbPath);
  t.after(() => {
    bus.close();
    rmSync(root, { recursive: true, force: true });
  });
  return {
    dbPath,
    bus,
    src: bus.addEndpoint("claude", "src", new Date(T0)),
    a: bus.addEndpoint("codex", "a", new Date(T0)),
    b: bus.addEndpoint("codex", "b", new Date(T0)),
    c: bus.addEndpoint("codex", "c", new Date(T0)),
    claudeConsumer: createConsumerId("claude"),
    codexConsumer: createConsumerId("codex"),
  };
}

function consumerFor(desk: Desk, endpoint: EndpointRow): string {
  return endpoint.role === "claude" ? desk.claudeConsumer : desk.codexConsumer;
}

function sendTo(
  desk: Desk,
  names: string[],
  opts: {
    expectsReply?: boolean;
    messageId?: string;
    now?: number;
    subject?: string;
    body?: string;
    source?: EndpointRow;
  } = {},
): string {
  const messageId = opts.messageId ?? randomUUID();
  const source = opts.source ?? desk.src;
  desk.bus.send({
    fromRole: source.role,
    toRole: source.role === "claude" ? "codex" : "claude",
    subject: opts.subject ?? "need",
    body: opts.body ?? "please",
    messageId,
    toEndpoints: names,
    sourceEndpoint: source,
    expectsReply: opts.expectsReply ?? false,
    now: opts.now ?? T0,
  });
  return messageId;
}

function reply(
  desk: Desk,
  from: EndpointRow,
  requestId: string,
  kind: "answer" | "decline" | "withdraw",
  opts: {
    body?: string;
    messageId?: string;
    expectsReply?: boolean;
    toEndpoints?: string[];
    subject?: string;
    now?: number;
  } = {},
): string {
  const messageId = opts.messageId ?? randomUUID();
  desk.bus.send({
    fromRole: from.role,
    toRole: from.role === "claude" ? "codex" : "claude",
    subject: opts.subject ?? "re",
    body: opts.body ?? "done",
    messageId,
    sourceEndpoint: from,
    inReplyTo: requestId,
    replyKind: kind,
    expectsReply: opts.expectsReply,
    toEndpoints: opts.toEndpoints,
    now: opts.now ?? T0,
  });
  return messageId;
}

function fetchAs(
  desk: Desk,
  endpoint: EndpointRow,
  peek: boolean,
  now = T0,
): FetchResult {
  return desk.bus.fetch(endpoint.role, consumerFor(desk, endpoint), {
    peek,
    limit: 10,
    endpoint,
    now,
  });
}

function acknowledge(
  desk: Desk,
  endpoint: EndpointRow,
  messageId: string,
  now = T0,
): void {
  const fetched = desk.bus.fetch(endpoint.role, consumerFor(desk, endpoint), {
    messageId,
    endpoint,
    now,
  });
  const attemptId = fetched.messages[0]?.attempt_id;
  assert.equal(typeof attemptId, "string");
  desk.bus.ack(
    endpoint.role,
    messageId,
    attemptId,
    now,
    consumerFor(desk, endpoint),
    endpoint,
  );
}

function withDb<T>(
  dbPath: string,
  write: boolean,
  work: (db: Database.Database) => T,
): T {
  const db = new Database(
    dbPath,
    write ? { fileMustExist: true } : { readonly: true, fileMustExist: true },
  );
  try {
    if (write) db.pragma("busy_timeout = 5000");
    return work(db);
  } finally {
    db.close();
  }
}

function counts(dbPath: string): { messages: number; deliveries: number; events: number } {
  return withDb(dbPath, false, (db) => ({
    messages: (db.prepare("SELECT COUNT(*) AS n FROM messages").get() as { n: number }).n,
    deliveries: (db.prepare("SELECT COUNT(*) AS n FROM deliveries").get() as { n: number }).n,
    events: (db.prepare("SELECT COUNT(*) AS n FROM events").get() as { n: number }).n,
  }));
}

function refuses(desk: Desk, run: () => void, pattern: RegExp): void {
  const before = counts(desk.dbPath);
  assert.throws(run, pattern);
  assert.deepEqual(counts(desk.dbPath), before);
}

function deliveryNames(dbPath: string, messageId: string): string[] {
  return withDb(dbPath, false, (db) =>
    (
      db
        .prepare(
          `SELECT ep.name AS name
             FROM deliveries d
             JOIN endpoints ep ON ep.endpoint_id = d.endpoint_id
            WHERE d.message_id = ?
            ORDER BY d.delivery_id`,
        )
        .all(messageId) as Array<{ name: string }>
    ).map((row) => row.name),
  );
}

function setState(
  dbPath: string,
  messageId: string,
  endpointId: string,
  state: string,
): void {
  withDb(dbPath, true, (db) => {
    const updated = db
      .prepare("UPDATE deliveries SET state = ? WHERE message_id = ? AND endpoint_id = ?")
      .run(state, messageId, endpointId);
    assert.equal(updated.changes, 1);
  });
}

function ledger(dbPath: string): string {
  return withDb(dbPath, false, (db) =>
    JSON.stringify({
      messages: db.prepare("SELECT * FROM messages ORDER BY id").all(),
      deliveries: db.prepare("SELECT * FROM deliveries ORDER BY delivery_id").all(),
      events: db.prepare("SELECT * FROM events ORDER BY seq").all(),
    }),
  );
}

function messageOf(
  dbPath: string,
  messageId: string,
): { expects_reply: number; reply_kind: string | null } {
  return withDb(
    dbPath,
    false,
    (db) =>
      db
        .prepare("SELECT expects_reply, reply_kind FROM messages WHERE message_id = ?")
        .get(messageId) as { expects_reply: number; reply_kind: string | null },
  );
}

async function toolStatus(
  desk: Desk,
  endpoint: EndpointRow,
  messageId: string,
): Promise<Record<string, unknown>> {
  const tools = new BridgeTools(
    desk.bus,
    endpoint.role,
    consumerFor(desk, endpoint),
    { tag: null },
    process.env,
    endpoint,
  );
  const result = await tools.call("bridge_status", { message_id: messageId });
  assert.equal(result.isError, undefined);
  return JSON.parse(result.content[0]!.text) as Record<string, unknown>;
}

function expectTerminal(t: TestContext, kind: "answer" | "decline"): void {
  const desk = openDesk(t);
  const requestId = sendTo(desk, ["a"], { expectsReply: true, subject: kind });
  acknowledge(desk, desk.a, requestId);
  const first = reply(desk, desk.a, requestId, kind, { body: "because" });
  assert.deepEqual(deliveryNames(desk.dbPath, first), ["src"]);
  assert.equal(
    fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === requestId),
    false,
  );
  const waiting = fetchAs(desk, desk.src, true);
  assert.deepEqual(
    waiting.awaiting.filter((row) => row.message_id === requestId).map((row) => row.to_endpoint),
    ["a"],
  );
  const peeked = waiting.messages.find((row) => row.message_id === first);
  assert.ok(peeked);
  assert.equal(peeked.expects_reply, false);
  assert.equal(peeked.in_reply_to, requestId);
  assert.equal(peeked.reply_kind, kind);
  desk.bus.cancelDeliveries({
    messageId: first,
    endpointName: "src",
    reason: "lost",
    now: T0,
  });
  assert.equal(
    fetchAs(desk, desk.src, true).awaiting.some((row) => row.message_id === requestId),
    true,
  );
  const second = reply(desk, desk.a, requestId, kind, { body: "again" });
  const claimed = desk.bus.fetch("claude", desk.claudeConsumer, {
    messageId: second,
    endpoint: desk.src,
    now: T0,
  });
  const row = claimed.messages[0];
  assert.ok(row);
  assert.equal(row.reply_kind, kind);
  assert.equal(row.in_reply_to, requestId);
  assert.equal(row.expects_reply, false);
  assert.equal(typeof row.attempt_id, "string");
  desk.bus.ack("claude", second, row.attempt_id, T0, desk.claudeConsumer, desk.src);
  assert.equal(
    fetchAs(desk, desk.src, true).awaiting.some((row) => row.message_id === requestId),
    false,
  );
}

test("f-1 owed begins at ack and awaiting begins at send", (t) => {
  const desk = openDesk(t);
  const quiet = sendTo(desk, ["a"], { expectsReply: false, subject: "note" });
  acknowledge(desk, desk.a, quiet);
  const id = sendTo(desk, ["a"], { expectsReply: true, subject: "need" });
  const expectedAwaiting = [
    { message_id: id, subject: "need", to_endpoint: "a", since: SENT },
  ];
  const waiting = fetchAs(desk, desk.src, true);
  const waitingClaim = fetchAs(desk, desk.src, false);
  assert.deepEqual(waiting.awaiting, expectedAwaiting);
  assert.equal(waiting.awaiting_total, 1);
  assert.deepEqual(waitingClaim.awaiting, expectedAwaiting);
  assert.equal(waiting.owed_total, 0);
  const pending = fetchAs(desk, desk.a, true);
  assert.equal(pending.owed_total, 0);
  const shown = pending.messages.find((row) => row.message_id === id);
  assert.ok(shown);
  assert.equal(shown.expects_reply, true);
  assert.equal(shown.in_reply_to, null);
  assert.equal(shown.reply_kind, null);
  const claimed = desk.bus.claim("codex", desk.codexConsumer, 1, T0, null, desk.a);
  assert.equal(claimed[0]?.message_id, id);
  assert.equal(fetchAs(desk, desk.a, true).owed_total, 0);
  assert.equal(fetchAs(desk, desk.a, false).owed_total, 0);
  desk.bus.markPresented(
    "codex",
    desk.codexConsumer,
    [{ messageId: id, attemptId: claimed[0]!.attempt_id }],
    T0,
    desk.a,
  );
  assert.equal(fetchAs(desk, desk.a, true).owed_total, 0);
  assert.equal(fetchAs(desk, desk.a, false).owed_total, 0);
  const ackedAt = T0 + 60_000;
  desk.bus.ack("codex", id, claimed[0]!.attempt_id, ackedAt, desk.codexConsumer, desk.a);
  const expectedOwed = [
    { message_id: id, subject: "need", from_endpoint: "src", since: iso(ackedAt) },
  ];
  const owed = fetchAs(desk, desk.a, true, ackedAt);
  const owedClaim = fetchAs(desk, desk.a, false, ackedAt);
  assert.deepEqual(owed.owed, expectedOwed);
  assert.equal(owed.owed_total, 1);
  assert.deepEqual(owedClaim.owed, expectedOwed);
  assert.equal(fetchAs(desk, desk.src, true, ackedAt).awaiting_total, 1);
  assert.equal(
    fetchAs(desk, desk.a, true, ackedAt).owed.some((row) => row.message_id === quiet),
    false,
  );
  assert.equal(
    fetchAs(desk, desk.src, true, ackedAt).awaiting.some((row) => row.message_id === quiet),
    false,
  );
});

test("f-2 answer and decline clear owed immediately and awaiting only after ack", (t) => {
  expectTerminal(t, "answer");
  expectTerminal(t, "decline");
});

test("f-3 withdraw clears every awaiting row now and owed only after ack", (t) => {
  const desk = openDesk(t);
  const id = sendTo(desk, ["a", "b"], { expectsReply: true, subject: "both" });
  acknowledge(desk, desk.a, id);
  const withdrawId = reply(desk, desk.src, id, "withdraw");
  assert.deepEqual(deliveryNames(desk.dbPath, withdrawId), ["a", "b"]);
  assert.equal(fetchAs(desk, desk.src, true).awaiting_total, 0);
  assert.equal(
    fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === id),
    true,
  );
  acknowledge(desk, desk.a, withdrawId);
  assert.equal(
    fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === id),
    false,
  );
});

test("f-4 one answer leaves the other recipient, and a resend adds a third", (t) => {
  const desk = openDesk(t);
  const id = sendTo(desk, ["a", "b"], { expectsReply: true });
  acknowledge(desk, desk.a, id);
  acknowledge(desk, desk.b, id);
  const answerId = reply(desk, desk.a, id, "answer");
  acknowledge(desk, desk.src, answerId);
  assert.deepEqual(
    fetchAs(desk, desk.src, true).awaiting.map((row) => row.to_endpoint),
    ["b"],
  );
  assert.equal(fetchAs(desk, desk.b, true).owed.some((row) => row.message_id === id), true);
  assert.equal(fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === id), false);
  const added = desk.bus.send({
    fromRole: "claude",
    toRole: "codex",
    subject: "need",
    body: "please",
    messageId: id,
    toEndpoints: ["a", "b", "c"],
    sourceEndpoint: desk.src,
    expectsReply: true,
    now: T0,
  });
  assert.deepEqual(added.added, ["c"]);
  assert.equal(fetchAs(desk, desk.c, true).owed_total, 0);
  acknowledge(desk, desk.c, id);
  assert.equal(fetchAs(desk, desk.c, true).owed.some((row) => row.message_id === id), true);
  assert.equal(fetchAs(desk, desk.b, true).owed.some((row) => row.message_id === id), true);
  assert.deepEqual(
    fetchAs(desk, desk.src, true).awaiting.map((row) => row.to_endpoint),
    ["b", "c"],
  );
});

test("f-5 cancelled, rejected, and bounced deliveries are neither owed nor awaited", (t) => {
  const desk = openDesk(t);
  for (const state of ["cancelled", "rejected", "bounced"] as const) {
    const id = sendTo(desk, ["a"], { expectsReply: true, subject: state });
    if (state === "cancelled") {
      desk.bus.cancelDeliveries({
        messageId: id,
        endpointName: "a",
        reason: state,
        now: T0,
      });
    } else {
      setState(desk.dbPath, id, desk.a.endpoint_id, state);
    }
  }
  assert.equal(fetchAs(desk, desk.src, true).awaiting_total, 0);
  assert.equal(fetchAs(desk, desk.a, true).owed_total, 0);
});

test("f-6 each send refusal names its rule and writes nothing", (t) => {
  const desk = openDesk(t);
  const plain = sendTo(desk, ["a"], { expectsReply: false, subject: "plain" });
  acknowledge(desk, desk.a, plain);
  const openReq = sendTo(desk, ["a"], { expectsReply: true, subject: "open" });
  const held = sendTo(desk, ["a"], { expectsReply: true, subject: "held" });
  acknowledge(desk, desk.a, held);
  const cases: Array<[() => void, RegExp]> = [
    [() => reply(desk, desk.a, randomUUID(), "answer"), /names no message/],
    [() => reply(desk, desk.a, plain, "answer"), /does not expect a reply/],
    [
      () =>
        desk.bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "need",
          body: "please",
          sourceEndpoint: desk.src,
          toEndpoints: ["a"],
          replyKind: "answer",
          now: T0,
        }),
      /reply_kind requires in_reply_to/,
    ],
    [
      () =>
        desk.bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "re",
          body: "done",
          sourceEndpoint: desk.a,
          inReplyTo: held,
          now: T0,
        }),
      /in_reply_to requires reply_kind/,
    ],
    [
      () => reply(desk, desk.a, held, "answer", { expectsReply: true }),
      /cannot expect a reply/,
    ],
    [
      () => reply(desk, desk.a, held, "answer", { toEndpoints: ["src"] }),
      /do not pass to_endpoints/,
    ],
    [() => reply(desk, desk.a, openReq, "answer"), /confirmed the request/],
    [() => reply(desk, desk.src, held, "answer"), /confirmed the request/],
    [() => reply(desk, desk.a, held, "withdraw"), /only the requester/],
    [() => reply(desk, desk.a, held, "decline", { body: "" }), /1 to 262144/],
  ];
  for (const [run, pattern] of cases) refuses(desk, run, pattern);

  const doomed = sendTo(desk, ["b"], { expectsReply: true, subject: "doomed" });
  desk.bus.cancelDeliveries({
    messageId: doomed,
    endpointName: "b",
    reason: "drop",
    now: T0,
  });
  refuses(
    desk,
    () => reply(desk, desk.src, doomed, "withdraw"),
    /no live recipient/,
  );

  const widen = randomUUID();
  sendTo(desk, ["a"], { expectsReply: true, messageId: widen, subject: "widen" });
  reply(desk, desk.src, widen, "withdraw");
  refuses(
    desk,
    () =>
      desk.bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "widen",
        body: "please",
        messageId: widen,
        toEndpoints: ["a", "c"],
        sourceEndpoint: desk.src,
        expectsReply: true,
        now: T0,
      }),
    /cannot add recipients/,
  );
  assert.equal(deliveryNames(desk.dbPath, widen).includes("c"), false);

  const src2 = desk.bus.addEndpoint("claude", "src2", new Date(T0));
  const owned = sendTo(desk, ["a"], {
    expectsReply: true,
    source: src2,
    subject: "owned",
  });
  acknowledge(desk, desk.a, owned);
  desk.bus.retireEndpoint("claude", "src2", new Date(T0));
  refuses(
    desk,
    () => reply(desk, desk.a, owned, "answer"),
    /requester endpoint is retired/,
  );
});

test("f-7 changing reply_kind or expects_reply on resend conflicts and keeps the row", (t) => {
  const desk = openDesk(t);
  const id = sendTo(desk, ["a"], { expectsReply: true, subject: "flip" });
  const before = counts(desk.dbPath);
  assert.throws(
    () => sendTo(desk, ["a"], { expectsReply: false, messageId: id, subject: "flip" }),
    /different envelope/,
  );
  const after = counts(desk.dbPath);
  assert.equal(after.messages, before.messages);
  assert.equal(after.deliveries, before.deliveries);
  assert.equal(after.events, before.events + 1);
  assert.equal(messageOf(desk.dbPath, id).expects_reply, 1);

  const requestId = sendTo(desk, ["a"], { expectsReply: true, subject: "parent" });
  acknowledge(desk, desk.a, requestId);
  const answerId = randomUUID();
  reply(desk, desk.a, requestId, "answer", {
    messageId: answerId,
    subject: "same",
    body: "same",
  });
  const mark = counts(desk.dbPath);
  assert.throws(
    () =>
      reply(desk, desk.a, requestId, "decline", {
        messageId: answerId,
        subject: "same",
        body: "same",
      }),
    /different envelope/,
  );
  const later = counts(desk.dbPath);
  assert.equal(later.messages, mark.messages);
  assert.equal(later.deliveries, mark.deliveries);
  assert.equal(later.events, mark.events + 1);
  assert.equal(messageOf(desk.dbPath, answerId).reply_kind, "answer");
});

test("f-8 two peeks match and messages, deliveries, and events stay put", (t) => {
  const desk = openDesk(t);
  const inbound = sendTo(desk, ["a"], { expectsReply: true, subject: "in" });
  acknowledge(desk, desk.a, inbound);
  desk.bus.send({
    fromRole: "codex",
    toRole: "claude",
    subject: "out",
    body: "please",
    messageId: randomUUID(),
    toEndpoints: ["src"],
    sourceEndpoint: desk.a,
    expectsReply: true,
    now: T0,
  });
  const before = ledger(desk.dbPath);
  const first = fetchAs(desk, desk.a, true);
  const second = fetchAs(desk, desk.a, true);
  assert.equal(first.owed_total, 1);
  assert.equal(first.awaiting_total, 1);
  assert.deepEqual(second, first);
  assert.equal(ledger(desk.dbPath), before);
});

test("f-9 bridge_status returns body only to the source and confirmed holders", async (t) => {
  const desk = openDesk(t);
  const id = sendTo(desk, ["a", "b"], { expectsReply: true, subject: "secret" });
  acknowledge(desk, desk.a, id);
  const asSrc = await toolStatus(desk, desk.src, id);
  const asA = await toolStatus(desk, desk.a, id);
  const asB = await toolStatus(desk, desk.b, id);
  const asC = await toolStatus(desk, desk.c, id);
  assert.equal(asSrc.body, "please");
  assert.equal(asSrc.expects_reply, true);
  assert.equal(asSrc.in_reply_to, null);
  assert.equal(asSrc.reply_kind, null);
  assert.equal(asA.body, "please");
  assert.equal(desk.bus.status(id, desk.src).body, "please");
  assert.equal(desk.bus.status(id, desk.a).body, "please");
  assert.equal("body" in asB, false);
  assert.equal("body" in asC, false);
  assert.equal("body" in desk.bus.status(id, desk.b), false);
  assert.equal("body" in desk.bus.status(id, desk.c), false);
  assert.equal("body" in desk.bus.status(id), false);
  assert.equal(desk.bus.status(id).expects_reply, true);
});

test("f-10 retire drops the owed or awaited pair and blocks the answer", (t) => {
  const desk = openDesk(t);
  const owedId = sendTo(desk, ["a"], { expectsReply: true, subject: "owe" });
  acknowledge(desk, desk.a, owedId);
  const waitedId = sendTo(desk, ["b"], { expectsReply: true, subject: "wait" });
  acknowledge(desk, desk.b, waitedId);
  assert.equal(
    fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === owedId),
    true,
  );
  desk.bus.retireEndpoint("codex", "b", new Date(T0));
  const waiting = fetchAs(desk, desk.src, true);
  assert.equal(waiting.awaiting.some((row) => row.to_endpoint === "b"), false);
  assert.equal(waiting.awaiting.some((row) => row.message_id === waitedId), false);
  assert.equal(waiting.awaiting.some((row) => row.to_endpoint === "a"), true);
  desk.bus.retireEndpoint("claude", "src", new Date(T0));
  assert.equal(
    fetchAs(desk, desk.a, true).owed.some((row) => row.message_id === owedId),
    false,
  );
  refuses(desk, () => reply(desk, desk.a, owedId, "answer"), /requester endpoint is retired/);
});

test("f-11 obligation pages are the oldest ten and ties break by delivery_id", (t) => {
  const desk = openDesk(t);
  const ids: string[] = [];
  for (let i = 0; i < 11; i += 1) {
    ids.push(
      sendTo(desk, ["a"], {
        expectsReply: true,
        subject: `n${i}`,
        now: T0 + i,
      }),
    );
  }
  for (let i = 0; i < 11; i += 1) {
    acknowledge(desk, desk.a, ids[i]!, i < 2 ? T0 + 10_000 : T0 + 20_000 + i);
  }
  const owedPage = fetchAs(desk, desk.a, true, T0 + 50_000);
  assert.equal(owedPage.owed_total, 11);
  assert.deepEqual(
    owedPage.owed.map((row) => row.message_id),
    ids.slice(0, 10),
  );
  assert.equal(owedPage.owed[0]?.since, owedPage.owed[1]?.since);
  const waiting = fetchAs(desk, desk.src, true, T0 + 50_000);
  assert.equal(waiting.awaiting_total, 11);
  assert.deepEqual(
    waiting.awaiting.map((row) => row.message_id),
    ids.slice(0, 10),
  );
});