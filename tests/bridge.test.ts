import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
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
  BridgeDatabaseError,
  BridgeTransitionError,
  CLAIM_LEASE_MS,
  LEGACY_SCHEMA_VERSION,
  PRESENTED_TTL_MS,
  SCHEMA_VERSION,
  TAG_TTL_MS,
  computeEnvelopeHash,
  createConsumerId,
  deriveBounceMessageId,
  initializeBridgeDatabaseAtPath,
  migrateBridgeDatabaseAtPath,
  sha256,
} from "../src/db.js";
import {
  BridgeTools,
  TOOL_DEFINITIONS,
} from "../src/tools.js";

const PROJECT_ROOT = resolve(
  fileURLToPath(new URL("..", import.meta.url)),
);
const SERVER_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "server.ts",
);
const HOOK_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "hook-notify.ts",
);
const INIT_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-init.ts",
);
const THIS_TEST_FILE =
  fileURLToPath(import.meta.url);
const T0 = Date.UTC(
  2026,
  7,
  30,
  0,
  0,
  0,
);

interface WorkerResult {
  messages: Array<{
    message_id: string;
    attempt_id: string;
  }>;
}

interface ProcessResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

async function runClaimWorker(): Promise<void> {
  const dbPath =
    process.env.BRIDGE_TEST_DB;
  const nowText =
    process.env.BRIDGE_TEST_NOW;
  const tag =
    process.env.BRIDGE_TEST_TAG ?? null;

  if (!dbPath || !nowText) {
    throw new Error(
      "claim worker environment is incomplete",
    );
  }

  await once(process.stdin, "data");

  const bus = BridgeBus.open(dbPath);
  try {
    const claimed = bus.claim(
      "codex",
      createConsumerId("codex"),
      1,
      Number(nowText),
      tag,
    );

    const result: WorkerResult = {
      messages: claimed.map(
        (message) => ({
          message_id: message.message_id,
          attempt_id: message.attempt_id,
        }),
      ),
    };

    process.stdout.write(
      JSON.stringify(result),
    );
  } finally {
    bus.close();
  }
}

if (
  process.env.BRIDGE_TEST_WORKER ===
  "claim"
) {
  try {
    await runClaimWorker();
  } catch (error) {
    process.stderr.write(
      `${
        error instanceof Error
          ? error.stack
          : String(error)
      }\n`,
    );
    process.exitCode = 1;
  }
} else {
  function makeDb(t: TestContext): {
    directory: string;
    dbPath: string;
  } {
    const directory = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-test-",
      ),
    );
    const dbPath = join(
      directory,
      "bridge.db",
    );

    t.after(() => {
      rmSync(directory, {
        recursive: true,
        force: true,
      });
    });

    initializeBridgeDatabaseAtPath(dbPath);
    return { directory, dbPath };
  }

  function makeProfileDb(
    t: TestContext,
  ): {
    userProfile: string;
    dbPath: string;
  } {
    const userProfile = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-hook-profile-",
      ),
    );
    const dbPath = join(
      userProfile,
      ".claude",
      "data",
      "agent-bridge",
      "bridge.db",
    );

    t.after(() => {
      rmSync(userProfile, {
        recursive: true,
        force: true,
      });
    });

    initializeBridgeDatabaseAtPath(dbPath);
    return { userProfile, dbPath };
  }

  const LEGACY_SCHEMA_SQL = `
CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

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

  function initializeLegacyDatabaseAtPath(
    dbPath: string,
  ): string {
    mkdirSync(dirname(dbPath), {
      recursive: true,
    });
    const rootId = randomUUID();
    const db = new Database(dbPath);

    try {
      db.pragma("journal_mode = WAL");
      const initialize = db.transaction(
        () => {
          db.exec(LEGACY_SCHEMA_SQL);
          const insert = db.prepare(
            "INSERT INTO meta (k, v) VALUES (?, ?)",
          );
          insert.run("root_id", rootId);
          insert.run(
            "schema_version",
            LEGACY_SCHEMA_VERSION,
          );
          insert.run(
            "created_at",
            new Date(T0).toISOString(),
          );
        },
      );
      initialize.immediate();
    } finally {
      db.close();
    }

    return rootId;
  }

  function makeLegacyProfileDb(
    t: TestContext,
  ): {
    userProfile: string;
    dbPath: string;
  } {
    const userProfile = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-legacy-profile-",
      ),
    );
    const dbPath = join(
      userProfile,
      ".claude",
      "data",
      "agent-bridge",
      "bridge.db",
    );

    t.after(() => {
      rmSync(userProfile, {
        recursive: true,
        force: true,
      });
    });

    initializeLegacyDatabaseAtPath(dbPath);
    return { userProfile, dbPath };
  }

  function insertLegacyMessage(
    dbPath: string,
    input: {
      messageId: string;
      subject: string;
      body: string;
    },
  ): void {
    const db = new Database(dbPath);

    try {
      const root = db
        .prepare(
          "SELECT v FROM meta WHERE k = 'root_id'",
        )
        .get() as { v: string };
      const sentAt =
        new Date(T0).toISOString();
      const envelopeHash = sha256(
        JSON.stringify([
          "claude",
          "codex",
          input.subject,
          input.body,
        ]),
      );

      const insert = db.transaction(() => {
        db.prepare(
          `INSERT INTO messages (
             message_id,
             root_id,
             from_role,
             to_role,
             subject,
             body,
             envelope_sha256,
             body_sha256,
             sender_thread_id,
             status,
             sent_at
           ) VALUES (
             ?,
             ?,
             'claude',
             'codex',
             ?,
             ?,
             ?,
             ?,
             NULL,
             'stored',
             ?
           )`,
        ).run(
          input.messageId,
          root.v,
          input.subject,
          input.body,
          envelopeHash,
          sha256(input.body),
          sentAt,
        );

        db.prepare(
          `INSERT INTO events (
             message_id,
             attempt_id,
             event,
             at,
             detail
           ) VALUES (?, NULL, 'sent', ?, NULL)`,
        ).run(input.messageId, sentAt);
      });

      insert.immediate();
    } finally {
      db.close();
    }
  }

  function countRows(
    dbPath: string,
    table: "messages" | "events",
  ): number {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      const row = db
        .prepare(
          `SELECT COUNT(*) AS count FROM ${table}`,
        )
        .get() as { count: number };
      return row.count;
    } finally {
      db.close();
    }
  }

  function countEvents(
    dbPath: string,
    messageId: string,
    event?: string,
  ): number {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      const row = event
        ? (db
            .prepare(
              `SELECT COUNT(*) AS count
                 FROM events
                WHERE message_id = ?
                  AND event = ?`,
            )
            .get(
              messageId,
              event,
            ) as { count: number })
        : (db
            .prepare(
              `SELECT COUNT(*) AS count
                 FROM events
                WHERE message_id = ?`,
            )
            .get(messageId) as {
            count: number;
          });

      return row.count;
    } finally {
      db.close();
    }
  }

  function messageSnapshot(
    dbPath: string,
    messageId: string,
  ): string {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      const row = db
        .prepare(
          "SELECT * FROM messages WHERE message_id = ?",
        )
        .get(messageId);

      return JSON.stringify(row);
    } finally {
      db.close();
    }
  }

  function databaseSnapshot(
    dbPath: string,
  ): string {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      return JSON.stringify({
        messages: db
          .prepare(
            "SELECT * FROM messages ORDER BY id",
          )
          .all(),
        events: db
          .prepare(
            "SELECT * FROM events ORDER BY seq",
          )
          .all(),
      });
    } finally {
      db.close();
    }
  }

  function legacyDatabaseSnapshot(
    dbPath: string,
  ): string {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      return JSON.stringify({
        schema: db
          .prepare(
            `SELECT type, name, tbl_name, sql
               FROM sqlite_master
              WHERE name NOT LIKE 'sqlite_%'
              ORDER BY type, name`,
          )
          .all(),
        meta: db
          .prepare(
            "SELECT * FROM meta ORDER BY k",
          )
          .all(),
        messages: db
          .prepare(
            "SELECT * FROM messages ORDER BY id",
          )
          .all(),
        events: db
          .prepare(
            "SELECT * FROM events ORDER BY seq",
          )
          .all(),
      });
    } finally {
      db.close();
    }
  }

  function openAsLegacyServer(
    dbPath: string,
  ): void {
    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      const schema = db
        .prepare(
          "SELECT v FROM meta WHERE k = 'schema_version'",
        )
        .get() as
        | { v: string }
        | undefined;

      if (
        schema?.v !==
        LEGACY_SCHEMA_VERSION
      ) {
        throw new Error(
          `unsupported schema_version ${schema?.v ?? "missing"}; expected ${LEGACY_SCHEMA_VERSION}`,
        );
      }

      db.prepare(
        "SELECT * FROM messages LIMIT 1",
      ).get();
    } finally {
      db.close();
    }
  }

  function spawnClaimChild(
    dbPath: string,
    tag?: string,
  ): {
    child: ChildProcessWithoutNullStreams;
    result: Promise<WorkerResult>;
  } {
    const child = spawn(
      process.execPath,
      [
        "--import",
        "tsx",
        THIS_TEST_FILE,
      ],
      {
        cwd: PROJECT_ROOT,
        env: {
          ...process.env,
          BRIDGE_TEST_WORKER: "claim",
          BRIDGE_TEST_DB: dbPath,
          BRIDGE_TEST_NOW: String(T0),
          ...(tag === undefined
            ? {}
            : {
                BRIDGE_TEST_TAG: tag,
              }),
        },
        stdio: ["pipe", "pipe", "pipe"],
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

    const result =
      (async (): Promise<WorkerResult> => {
        const [code] = (await once(
          child,
          "close",
        )) as [
          number | null,
          NodeJS.Signals | null,
        ];

        assert.equal(
          code,
          0,
          `claim worker failed: ${stderr}`,
        );

        return JSON.parse(
          stdout,
        ) as WorkerResult;
      })();

    return { child, result };
  }

  async function runTypeScriptProcess(
    entry: string,
    args: readonly string[],
    userProfile: string,
    stdin?: string,
  ): Promise<ProcessResult> {
    const child = spawn(
      process.execPath,
      [
        "--import",
        "tsx",
        entry,
        ...args,
      ],
      {
        cwd: PROJECT_ROOT,
        env: {
          ...process.env,
          USERPROFILE: userProfile,
        },
        stdio: ["pipe", "pipe", "pipe"],
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

    child.stdin.end(stdin);

    const [code] = (await once(
      child,
      "close",
    )) as [
      number | null,
      NodeJS.Signals | null,
    ];

    return { code, stdout, stderr };
  }

  async function runServerProcess(
    role: "claude" | "codex",
    userProfile: string,
  ): Promise<ProcessResult> {
    return runTypeScriptProcess(
      SERVER_ENTRY,
      ["--role", role],
      userProfile,
    );
  }

  async function runHookProcess(
    event:
      | "stop"
      | "user-prompt-submit",
    userProfile: string,
    payload: object | string,
  ): Promise<ProcessResult> {
    return runTypeScriptProcess(
      HOOK_ENTRY,
      ["--event", event],
      userProfile,
      typeof payload === "string"
        ? payload
        : JSON.stringify(payload),
    );
  }

  async function runBridgeInitProcess(
    userProfile: string,
    args: readonly string[],
  ): Promise<ProcessResult> {
    return runTypeScriptProcess(
      INIT_ENTRY,
      args,
      userProfile,
    );
  }

  function extractHookNotice(
    stdout: string,
  ): string {
    const parsed = JSON.parse(stdout) as {
      reason?: unknown;
      hookSpecificOutput?: {
        additionalContext?: unknown;
      };
    };

    const notice =
      typeof parsed.reason === "string"
        ? parsed.reason
        : parsed.hookSpecificOutput
            ?.additionalContext;

    assert.equal(
      typeof notice,
      "string",
    );
    return notice as string;
  }

  /*
   * Existing v3.2/v4.2 acceptance coverage.
   */

  test(
    "1: two processes race to claim and exactly one transition wins",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "race",
        body: "one row",
        messageId,
        now: T0,
      });
      bus.close();

      const workerA =
        spawnClaimChild(dbPath);
      const workerB =
        spawnClaimChild(dbPath);

      workerA.child.stdin.end("go\n");
      workerB.child.stdin.end("go\n");

      const [resultA, resultB] =
        await Promise.all([
          workerA.result,
          workerB.result,
        ]);

      assert.equal(
        resultA.messages.length +
          resultB.messages.length,
        1,
      );
      assert.equal(
        countEvents(
          dbPath,
          messageId,
          "claimed",
        ),
        1,
      );

      const verify =
        BridgeBus.open(dbPath);
      try {
        const row =
          verify.readMessage(messageId);
        assert.equal(
          row?.status,
          "claimed",
        );
        assert.equal(
          row?.attempt_count,
          1,
        );
      } finally {
        verify.close();
      }
    },
  );

  test(
    "2: a live claim is not stolen and is recovered only after lease expiry",
    (t) => {
      const { dbPath } = makeDb(t);
      const busA = BridgeBus.open(dbPath);
      const busB = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        busA.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "lease",
          body: "do not steal",
          messageId,
          now: T0,
        });

        const first = busA.claim(
          "codex",
          createConsumerId("codex"),
          1,
          T0,
        );
        assert.equal(first.length, 1);

        const eventsBefore = countEvents(
          dbPath,
          messageId,
        );
        const earlyRecovery = busB.recover(
          "codex",
          T0 + CLAIM_LEASE_MS - 1,
        );
        const earlyClaim = busB.claim(
          "codex",
          createConsumerId("codex"),
          1,
          T0 + CLAIM_LEASE_MS - 1,
        );

        assert.deepEqual(
          earlyRecovery,
          {
            leaseExpired: 0,
            requeued: 0,
          },
        );
        assert.equal(
          earlyClaim.length,
          0,
        );
        assert.equal(
          countEvents(dbPath, messageId),
          eventsBefore,
        );

        const recoveryTime =
          T0 + CLAIM_LEASE_MS + 1;
        const recovered = busB.recover(
          "codex",
          recoveryTime,
        );
        const second = busB.claim(
          "codex",
          createConsumerId("codex"),
          1,
          recoveryTime,
        );

        assert.equal(
          recovered.leaseExpired,
          1,
        );
        assert.equal(second.length, 1);
        assert.equal(
          second[0]?.redelivery,
          true,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "lease_expired",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "claimed",
          ),
          2,
        );
        assert.equal(
          busB.readMessage(messageId)
            ?.attempt_count,
          2,
        );
      } finally {
        busA.close();
        busB.close();
      }
    },
  );

  test(
    "3: hook-boundary crash injection preserves or recovers delivery",
    async (t) => {
      await t.test(
        "failure before hook output changes no state",
        async (subtest) => {
          const {
            userProfile,
            dbPath,
          } = makeProfileDb(subtest);
          const bus =
            BridgeBus.open(dbPath);
          const messageId =
            randomUUID();

          bus.send({
            fromRole: "codex",
            toRole: "claude",
            subject:
              "before hook output",
            body: "must remain stored",
            messageId,
            now: T0,
          });
          bus.close();

          const before =
            databaseSnapshot(dbPath);
          const result =
            await runHookProcess(
              "stop",
              userProfile,
              "{",
            );

          assert.equal(result.code, 0);
          assert.equal(
            result.stdout,
            "",
          );
          assert.match(
            result.stderr,
            /agent-bridge hook skipped/,
          );
          assert.equal(
            databaseSnapshot(dbPath),
            before,
          );
        },
      );

      await t.test(
        "death after hook output and before model action changes no state",
        async (subtest) => {
          const {
            userProfile,
            dbPath,
          } = makeProfileDb(subtest);
          const bus =
            BridgeBus.open(dbPath);
          const messageId =
            randomUUID();
          const subject =
            "do-not-leak-subject";
          const body =
            "do-not-leak-body";

          bus.send({
            fromRole: "codex",
            toRole: "claude",
            subject,
            body,
            messageId,
            now: T0,
          });
          bus.close();

          const before =
            databaseSnapshot(dbPath);
          const result =
            await runHookProcess(
              "stop",
              userProfile,
              {
                hook_event_name:
                  "Stop",
                stop_hook_active:
                  false,
              },
            );

          assert.equal(result.code, 0);
          assert.equal(
            result.stderr,
            "",
          );
          assert.match(
            extractHookNotice(
              result.stdout,
            ),
            /untagged=1/,
          );
          assert.doesNotMatch(
            result.stdout,
            new RegExp(subject),
          );
          assert.doesNotMatch(
            result.stdout,
            new RegExp(body),
          );
          assert.equal(
            databaseSnapshot(dbPath),
            before,
          );
        },
      );

      await t.test(
        "death between claim and presented recovers through hook then fetch",
        async (subtest) => {
          const {
            userProfile,
            dbPath,
          } = makeProfileDb(subtest);
          const bus =
            BridgeBus.open(dbPath);
          const messageId =
            randomUUID();
          const past =
            Date.now() -
            CLAIM_LEASE_MS -
            2_000;

          try {
            bus.send({
              fromRole: "codex",
              toRole: "claude",
              subject:
                "claim boundary",
              body:
                "recover via fetch",
              messageId,
              now: past,
            });

            const first = bus.claim(
              "claude",
              createConsumerId(
                "claude",
              ),
              1,
              past,
            );
            assert.equal(
              first.length,
              1,
            );

            const eventsBeforeHook =
              countEvents(
                dbPath,
                messageId,
              );
            const notice =
              await runHookProcess(
                "stop",
                userProfile,
                {
                  hook_event_name:
                    "Stop",
                  stop_hook_active:
                    false,
                },
              );

            assert.equal(
              notice.code,
              0,
            );
            assert.equal(
              notice.stderr,
              "",
            );
            assert.match(
              extractHookNotice(
                notice.stdout,
              ),
              /期限切れclaimed=1/,
            );
            assert.equal(
              countEvents(
                dbPath,
                messageId,
              ),
              eventsBeforeHook,
            );

            const retry = bus.fetch(
              "claude",
              createConsumerId(
                "claude",
              ),
              {
                limit: 1,
                now: Date.now(),
              },
            );

            assert.equal(
              retry.messages.length,
              1,
            );
            assert.notEqual(
              retry.messages[0]!
                .attempt_id,
              first[0]!.attempt_id,
            );
            assert.equal(
              countEvents(
                dbPath,
                messageId,
              ),
              eventsBeforeHook + 3,
            );

            bus.ack(
              "claude",
              messageId,
              retry.messages[0]!
                .attempt_id!,
            );

            const status =
              bus.status(messageId);
            assert.equal(
              status.message.status,
              "acked",
            );
            assert.equal(
              status.event_counts
                .claimed,
              2,
            );
            assert.equal(
              status.event_counts
                .lease_expired,
              1,
            );
            assert.equal(
              status.event_counts
                .presented,
              1,
            );
            assert.equal(
              status.event_counts.acked,
              1,
            );
          } finally {
            bus.close();
          }
        },
      );

      await t.test(
        "death after presented and before ack recovers through hook then fetch",
        async (subtest) => {
          const {
            userProfile,
            dbPath,
          } = makeProfileDb(subtest);
          const bus =
            BridgeBus.open(dbPath);
          const messageId =
            randomUUID();
          const past =
            Date.now() -
            PRESENTED_TTL_MS -
            2_000;

          try {
            bus.send({
              fromRole: "codex",
              toRole: "claude",
              subject:
                "presented boundary",
              body:
                "recover via fetch",
              messageId,
              now: past,
            });

            const first = bus.fetch(
              "claude",
              createConsumerId(
                "claude",
              ),
              {
                limit: 1,
                now: past,
              },
            );
            assert.equal(
              first.messages.length,
              1,
            );

            const eventsBeforeHook =
              countEvents(
                dbPath,
                messageId,
              );
            const notice =
              await runHookProcess(
                "stop",
                userProfile,
                {
                  hook_event_name:
                    "Stop",
                  stop_hook_active:
                    false,
                },
              );

            assert.equal(
              notice.code,
              0,
            );
            assert.equal(
              notice.stderr,
              "",
            );
            assert.match(
              extractHookNotice(
                notice.stdout,
              ),
              /期限切れpresented=1/,
            );
            assert.equal(
              countEvents(
                dbPath,
                messageId,
              ),
              eventsBeforeHook,
            );

            const retry = bus.fetch(
              "claude",
              createConsumerId(
                "claude",
              ),
              {
                limit: 1,
                now: Date.now(),
              },
            );

            assert.equal(
              retry.messages.length,
              1,
            );
            assert.notEqual(
              retry.messages[0]!
                .attempt_id,
              first.messages[0]!
                .attempt_id,
            );
            assert.equal(
              countEvents(
                dbPath,
                messageId,
              ),
              eventsBeforeHook + 3,
            );

            bus.ack(
              "claude",
              messageId,
              retry.messages[0]!
                .attempt_id!,
            );

            const status =
              bus.status(messageId);
            assert.equal(
              status.message.status,
              "acked",
            );
            assert.equal(
              status.event_counts
                .claimed,
              2,
            );
            assert.equal(
              status.event_counts
                .presented,
              2,
            );
            assert.equal(
              status.event_counts
                .requeued,
              1,
            );
            assert.equal(
              status.event_counts.acked,
              1,
            );
          } finally {
            bus.close();
          }
        },
      );
    },
  );

  test(
    "4: ack rejects pre-presentation, stale attempts, and the wrong role",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "ack guards",
          body: "strict",
          messageId,
          now: T0,
        });

        const sentEvents = countEvents(
          dbPath,
          messageId,
        );

        assert.throws(
          () =>
            bus.ack(
              "codex",
              messageId,
              randomUUID(),
              T0,
            ),
          BridgeTransitionError,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
          ),
          sentEvents,
        );

        const firstConsumer =
          createConsumerId("codex");
        const first = bus.claim(
          "codex",
          firstConsumer,
          1,
          T0,
        );
        bus.markPresented(
          "codex",
          firstConsumer,
          [
            {
              messageId,
              attemptId:
                first[0]!.attempt_id,
            },
          ],
          T0,
        );

        const retryTime =
          T0 + PRESENTED_TTL_MS + 1;
        bus.recover(
          "codex",
          retryTime,
        );

        const secondConsumer =
          createConsumerId("codex");
        const second = bus.claim(
          "codex",
          secondConsumer,
          1,
          retryTime,
        );
        bus.markPresented(
          "codex",
          secondConsumer,
          [
            {
              messageId,
              attemptId:
                second[0]!.attempt_id,
            },
          ],
          retryTime,
        );

        const beforeInvalidAcks =
          countEvents(
            dbPath,
            messageId,
          );

        assert.throws(
          () =>
            bus.ack(
              "codex",
              messageId,
              first[0]!.attempt_id,
              retryTime,
            ),
          (error: unknown) => {
            assert.ok(
              error instanceof
                BridgeTransitionError,
            );
            assert.equal(
              error.latest?.attempt_id,
              second[0]!.attempt_id,
            );
            return true;
          },
        );

        assert.throws(
          () =>
            bus.ack(
              "claude",
              messageId,
              second[0]!.attempt_id,
              retryTime,
            ),
          BridgeTransitionError,
        );

        assert.equal(
          countEvents(
            dbPath,
            messageId,
          ),
          beforeInvalidAcks,
        );

        bus.ack(
          "codex",
          messageId,
          second[0]!.attempt_id,
          retryTime,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "acked",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "5: root and body poison rows are rejected without starving later rows",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const rootPoison = randomUUID();
      const bodyPoison = randomUUID();
      const good = randomUUID();

      try {
        for (const [
          messageId,
          subject,
        ] of [
          [rootPoison, "root poison"],
          [bodyPoison, "body poison"],
          [good, "good"],
        ] as const) {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject,
            body: "body",
            messageId,
            now: T0,
          });
        }

        const tamper =
          new Database(dbPath);
        try {
          tamper
            .prepare(
              "UPDATE messages SET root_id = ? WHERE message_id = ?",
            )
            .run(
              "wrong-root",
              rootPoison,
            );
          tamper
            .prepare(
              "UPDATE messages SET body = body || ? WHERE message_id = ?",
            )
            .run(
              "tampered",
              bodyPoison,
            );
        } finally {
          tamper.close();
        }

        const consumer =
          createConsumerId("codex");
        const claimed = bus.claim(
          "codex",
          consumer,
          10,
          T0,
        );

        assert.deepEqual(
          claimed.map(
            (message) =>
              message.message_id,
          ),
          [good],
        );
        assert.equal(
          bus.readMessage(rootPoison)
            ?.status,
          "rejected",
        );
        assert.equal(
          bus.readMessage(bodyPoison)
            ?.status,
          "rejected",
        );
        assert.equal(
          countEvents(
            dbPath,
            rootPoison,
            "rejected",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            bodyPoison,
            "rejected",
          ),
          1,
        );

        bus.markPresented(
          "codex",
          consumer,
          [
            {
              messageId: good,
              attemptId:
                claimed[0]!.attempt_id,
            },
          ],
          T0,
        );
        bus.ack(
          "codex",
          good,
          claimed[0]!.attempt_id,
          T0,
        );

        assert.equal(
          bus.readMessage(good)?.status,
          "acked",
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "6: idempotency, conflicts, lost responses, and envelope boundaries",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        const first = bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "same",
          body: "payload",
          messageId,
          now: T0,
        });
        assert.equal(
          first.idempotent,
          false,
        );

        const messagesAfterFirst =
          countRows(
            dbPath,
            "messages",
          );
        const eventsAfterFirst =
          countRows(dbPath, "events");

        const repeated = bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "same",
          body: "payload",
          messageId,
          now: T0 + 1,
        });

        assert.equal(
          repeated.idempotent,
          true,
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          messagesAfterFirst,
        );
        assert.equal(
          countRows(dbPath, "events"),
          eventsAfterFirst,
        );

        assert.throws(
          () =>
            bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject: "different",
              body: "payload",
              messageId,
              now: T0 + 2,
            }),
          BridgeConflictError,
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          messagesAfterFirst,
        );
        assert.equal(
          countRows(dbPath, "events"),
          eventsAfterFirst + 1,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "send_conflict",
          ),
          1,
        );

        const uncertainResponseId =
          randomUUID();
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "response lost",
          body:
            "retry with same id",
          messageId:
            uncertainResponseId,
          now: T0,
        });
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "response lost",
          body:
            "retry with same id",
          messageId:
            uncertainResponseId,
          now: T0 + 1,
        });

        assert.equal(
          countEvents(
            dbPath,
            uncertainResponseId,
            "sent",
          ),
          1,
        );

        const boundaryA =
          computeEnvelopeHash(
            "claude",
            "codex",
            "a|b",
            "c",
          );
        const boundaryB =
          computeEnvelopeHash(
            "claude",
            "codex",
            "a",
            "b|c",
          );
        assert.notEqual(
          boundaryA,
          boundaryB,
        );

        const normalized = bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "\n a\u0000b \r",
          body: "normalization",
          messageId: randomUUID(),
          now: T0,
        });
        assert.equal(
          normalized.subject,
          "a b",
        );

        assert.doesNotThrow(() =>
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "あ".repeat(166),
            body:
              "498-byte subject",
            messageId: randomUUID(),
            now: T0,
          }),
        );

        assert.throws(() =>
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "あ".repeat(167),
            body:
              "501-byte subject",
            messageId: randomUUID(),
            now: T0,
          }),
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "7: a conflict against an acked row leaves every message column unchanged",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();
      const consumer =
        createConsumerId("codex");

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "terminal",
          body: "immutable",
          messageId,
          now: T0,
        });

        const claimed = bus.claim(
          "codex",
          consumer,
          1,
          T0,
        );
        bus.markPresented(
          "codex",
          consumer,
          [
            {
              messageId,
              attemptId:
                claimed[0]!.attempt_id,
            },
          ],
          T0,
        );
        bus.ack(
          "codex",
          messageId,
          claimed[0]!.attempt_id,
          T0,
        );

        const before =
          messageSnapshot(
            dbPath,
            messageId,
          );

        assert.throws(
          () =>
            bus.send({
              fromRole: "claude",
              toRole: "codex",
              subject:
                "terminal changed",
              body: "immutable",
              messageId,
              now: T0 + 1,
            }),
          BridgeConflictError,
        );

        const after =
          messageSnapshot(
            dbPath,
            messageId,
          );

        assert.equal(after, before);
        assert.equal(
          bus.readMessage(messageId)
            ?.status,
          "acked",
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "rejected",
          ),
          0,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "send_conflict",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "8: 12 messages page in id order with correct has_more transitions",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const consumer =
        createConsumerId("codex");
      const expected: string[] = [];

      try {
        for (
          let index = 0;
          index < 12;
          index += 1
        ) {
          const messageId =
            randomUUID();
          expected.push(messageId);
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: `page ${index}`,
            body: `body ${index}`,
            messageId,
            now: T0 + index,
          });
        }

        const received: string[] = [];
        const hasMore: boolean[] = [];

        do {
          const page = bus.fetch(
            "codex",
            consumer,
            {
              limit: 3,
              now: T0 + 100,
            },
          );

          received.push(
            ...page.messages.map(
              (message) =>
                message.message_id,
            ),
          );
          hasMore.push(
            page.has_more,
          );
          assert.equal(
            page.unacked_total,
            12,
          );
        } while (hasMore.at(-1));

        assert.deepEqual(
          received,
          expected,
        );
        assert.deepEqual(hasMore, [
          true,
          true,
          true,
          false,
        ]);
        assert.equal(
          new Set(received).size,
          12,
        );

        for (const id of expected) {
          assert.equal(
            countEvents(
              dbPath,
              id,
              "claimed",
            ),
            1,
          );
          assert.equal(
            countEvents(
              dbPath,
              id,
              "presented",
            ),
            1,
          );
          assert.equal(
            bus.readMessage(id)
              ?.attempt_count,
            1,
          );
        }
      } finally {
        bus.close();
      }
    },
  );

  test(
    "9: cold-start peek uses the tool boundary and changes no message or event rows",
    async (t) => {
      const { dbPath } = makeDb(t);
      const producer =
        BridgeBus.open(dbPath);
      const firstId = randomUUID();
      const secondId = randomUUID();

      producer.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "peek one",
        body: "one",
        messageId: firstId,
        now: T0,
      });
      producer.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "peek two",
        body: "two",
        messageId: secondId,
        now: T0,
      });
      producer.close();

      const messagesBefore = countRows(
        dbPath,
        "messages",
      );
      const eventsBefore = countRows(
        dbPath,
        "events",
      );

      const coldStart =
        BridgeBus.open(dbPath);
      try {
        const tools = new BridgeTools(
          coldStart,
          "codex",
          createConsumerId("codex"),
        );

        const firstPeek =
          await tools.call(
            "bridge_fetch",
            {
              peek: true,
              limit: 1,
            },
          );
        const secondPeek =
          await tools.call(
            "bridge_fetch",
            {
              peek: true,
              limit: 1,
            },
          );

        assert.equal(
          firstPeek.isError,
          undefined,
        );
        assert.equal(
          secondPeek.isError,
          undefined,
        );
        assert.match(
          firstPeek.content[0]!.text,
          /PEEK（状態不変/,
        );
        assert.match(
          firstPeek.content[0]!.text,
          new RegExp(firstId),
        );
        assert.match(
          secondPeek.content[0]!.text,
          new RegExp(firstId),
        );
      } finally {
        coldStart.close();
      }

      assert.equal(
        countRows(
          dbPath,
          "messages",
        ),
        messagesBefore,
      );
      assert.equal(
        countRows(dbPath, "events"),
        eventsBefore,
      );
    },
  );

  test(
    "10: both server roles reject an uninitialized database and start after bridge-init",
    async (t) => {
      const userProfile = mkdtempSync(
        join(
          tmpdir(),
          "agent-bridge-profile-",
        ),
      );

      t.after(() => {
        rmSync(userProfile, {
          recursive: true,
          force: true,
        });
      });

      const dbPath = join(
        userProfile,
        ".claude",
        "data",
        "agent-bridge",
        "bridge.db",
      );

      const [
        missingClaude,
        missingCodex,
      ] = await Promise.all([
        runServerProcess(
          "claude",
          userProfile,
        ),
        runServerProcess(
          "codex",
          userProfile,
        ),
      ]);

      assert.notEqual(
        missingClaude.code,
        0,
      );
      assert.notEqual(
        missingCodex.code,
        0,
      );
      assert.match(
        missingClaude.stderr,
        /run bridge-init first/,
      );
      assert.match(
        missingCodex.stderr,
        /run bridge-init first/,
      );
      assert.equal(
        existsSync(dbPath),
        false,
      );

      initializeBridgeDatabaseAtPath(
        dbPath,
      );

      const [
        startedClaude,
        startedCodex,
      ] = await Promise.all([
        runServerProcess(
          "claude",
          userProfile,
        ),
        runServerProcess(
          "codex",
          userProfile,
        ),
      ]);

      assert.equal(
        startedClaude.code,
        0,
        startedClaude.stderr,
      );
      assert.equal(
        startedCodex.code,
        0,
        startedCodex.stderr,
      );
      assert.equal(
        startedClaude.stdout,
        "",
      );
      assert.equal(
        startedCodex.stdout,
        "",
      );
      assert.match(
        startedClaude.stderr,
        /pid=\d+.*root_id=.*schema_version=4\.0/,
      );
      assert.match(
        startedCodex.stderr,
        /pid=\d+.*root_id=.*schema_version=4\.0/,
      );

      const emptyPath = join(
        dirname(dbPath),
        "empty.db",
      );
      new Database(emptyPath).close();
      assert.throws(
        () =>
          BridgeBus.open(emptyPath),
        BridgeDatabaseError,
      );

      const wrongSchemaPath = join(
        dirname(dbPath),
        "wrong-schema.db",
      );
      initializeBridgeDatabaseAtPath(
        wrongSchemaPath,
      );

      const tamper = new Database(
        wrongSchemaPath,
      );
      try {
        tamper
          .prepare(
            "UPDATE meta SET v = ? WHERE k = 'schema_version'",
          )
          .run("999");
      } finally {
        tamper.close();
      }

      assert.throws(
        () =>
          BridgeBus.open(
            wrongSchemaPath,
          ),
        /unsupported schema_version/,
      );
    },
  );

  test(
    "11: multibyte, maximum, and control-character bodies round-trip through bridge_fetch",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const multibyteId =
        randomUUID();
      const maximumId = randomUUID();
      const controlId = randomUUID();
      const multibyteBody =
        "あ".repeat(3_000);
      const maximumBody =
        "a".repeat(262_144);
      const controlBody =
        "\u0000".repeat(262_144);
      const tools = new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
      );

      try {
        assert.equal(
          Buffer.byteLength(
            multibyteBody,
            "utf8",
          ),
          9_000,
        );
        assert.equal(
          Buffer.byteLength(
            maximumBody,
            "utf8",
          ),
          262_144,
        );
        assert.equal(
          Buffer.byteLength(
            controlBody,
            "utf8",
          ),
          262_144,
        );

        for (const [
          messageId,
          subject,
          body,
        ] of [
          [
            multibyteId,
            "multibyte",
            multibyteBody,
          ],
          [
            maximumId,
            "maximum",
            maximumBody,
          ],
          [
            controlId,
            "control characters",
            controlBody,
          ],
        ] as const) {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject,
            body,
            messageId,
            now: Date.now(),
          });
        }

        const fetched =
          await tools.call(
            "bridge_fetch",
            {
              limit: 3,
            },
          );

        assert.equal(
          fetched.isError,
          undefined,
        );

        const payload = JSON.parse(
          fetched.content[0]!.text,
        ) as {
          messages: Array<{
            message_id: string;
            attempt_id: string;
            body: string;
          }>;
        };

        assert.equal(
          payload.messages.length,
          3,
        );

        const byId = new Map(
          payload.messages.map(
            (message) => [
              message.message_id,
              message,
            ],
          ),
        );

        assert.equal(
          byId.get(multibyteId)
            ?.body,
          multibyteBody,
        );
        assert.equal(
          byId.get(maximumId)?.body,
          maximumBody,
        );
        assert.equal(
          byId.get(controlId)?.body,
          controlBody,
        );

        assert.equal(
          bus.status(multibyteId)
            .message.body_sha256,
          sha256(multibyteBody),
        );
        assert.equal(
          bus.status(maximumId)
            .message.body_sha256,
          sha256(maximumBody),
        );
        assert.equal(
          bus.status(controlId).message
            .body_sha256,
          sha256(controlBody),
        );

        for (
          const message of payload.messages
        ) {
          bus.ack(
            "codex",
            message.message_id,
            message.attempt_id,
            Date.now(),
          );
          assert.equal(
            bus.status(
              message.message_id,
            ).message.status,
            "acked",
          );
        }

        const messagesBefore =
          countRows(
            dbPath,
            "messages",
          );
        const eventsBefore =
          countRows(dbPath, "events");

        assert.throws(() =>
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: "too large",
            body: "a".repeat(262_145),
            messageId: randomUUID(),
            now: T0,
          }),
        );

        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          messagesBefore,
        );
        assert.equal(
          countRows(dbPath, "events"),
          eventsBefore,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "13: hook notice and bridge_fetch production path recover all pending categories",
    async (t) => {
      const {
        userProfile,
        dbPath,
      } = makeProfileDb(t);
      const bus = BridgeBus.open(dbPath);

      try {
        const storedId = randomUUID();
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "stored notice",
          body: "fetch and ack",
          messageId: storedId,
          now: Date.now(),
        });

        const beforeStoredHook =
          databaseSnapshot(dbPath);
        const storedHook =
          await runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          );

        assert.equal(
          storedHook.code,
          0,
        );
        assert.equal(
          storedHook.stderr,
          "",
        );

        const stopOutput = JSON.parse(
          storedHook.stdout,
        ) as Record<string, unknown>;
        assert.deepEqual(
          Object.keys(
            stopOutput,
          ).sort(),
          ["decision", "reason"],
        );
        assert.equal(
          stopOutput.decision,
          "block",
        );
        assert.equal(
          "systemMessage" in stopOutput,
          false,
        );
        assert.match(
          extractHookNotice(
            storedHook.stdout,
          ),
          /untagged=1/,
        );
        assert.equal(
          databaseSnapshot(dbPath),
          beforeStoredHook,
        );

        const storedFetch = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            limit: 1,
            now: Date.now(),
          },
        );
        assert.equal(
          storedFetch.messages.length,
          1,
        );
        bus.ack(
          "claude",
          storedId,
          storedFetch.messages[0]!
            .attempt_id!,
        );

        const afterAck =
          await runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          );
        assert.equal(
          afterAck.code,
          0,
        );
        assert.equal(
          afterAck.stdout,
          "",
        );
        assert.equal(
          afterAck.stderr,
          "",
        );

        const freshPresentedId =
          randomUUID();
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject:
            "fresh presented",
          body: "not expired",
          messageId:
            freshPresentedId,
          now: Date.now(),
        });
        const freshPresented =
          bus.fetch(
            "claude",
            createConsumerId(
              "claude",
            ),
            {
              limit: 1,
              now: Date.now(),
            },
          );

        const withinTtl =
          await runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          );
        assert.equal(
          withinTtl.code,
          0,
        );
        assert.equal(
          withinTtl.stdout,
          "",
        );
        assert.equal(
          withinTtl.stderr,
          "",
        );

        bus.ack(
          "claude",
          freshPresentedId,
          freshPresented.messages[0]!
            .attempt_id!,
        );

        const stalePresentedId =
          randomUUID();
        const stalePresentedAt =
          Date.now() -
          PRESENTED_TTL_MS -
          2_000;

        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject:
            "stale presented",
          body:
            "recover through bridge_fetch",
          messageId:
            stalePresentedId,
          now: stalePresentedAt,
        });

        const staleFirst = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            limit: 1,
            now: stalePresentedAt,
          },
        );
        const oldPresentedAttempt =
          staleFirst.messages[0]!
            .attempt_id!;

        const beforePresentedHook =
          countEvents(
            dbPath,
            stalePresentedId,
          );
        const presentedHook =
          await runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          );

        assert.equal(
          presentedHook.code,
          0,
        );
        assert.equal(
          presentedHook.stderr,
          "",
        );
        assert.match(
          extractHookNotice(
            presentedHook.stdout,
          ),
          /期限切れpresented=1/,
        );
        assert.equal(
          countEvents(
            dbPath,
            stalePresentedId,
          ),
          beforePresentedHook,
        );

        const staleRetry = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            limit: 1,
            now: Date.now(),
          },
        );

        assert.equal(
          staleRetry.messages.length,
          1,
        );
        assert.notEqual(
          staleRetry.messages[0]!
            .attempt_id,
          oldPresentedAttempt,
        );
        assert.equal(
          countEvents(
            dbPath,
            stalePresentedId,
          ),
          beforePresentedHook + 3,
        );

        const beforeOldAck =
          countEvents(
            dbPath,
            stalePresentedId,
          );
        assert.throws(
          () =>
            bus.ack(
              "claude",
              stalePresentedId,
              oldPresentedAttempt,
            ),
          BridgeTransitionError,
        );
        assert.equal(
          countEvents(
            dbPath,
            stalePresentedId,
          ),
          beforeOldAck,
        );

        bus.ack(
          "claude",
          stalePresentedId,
          staleRetry.messages[0]!
            .attempt_id!,
        );

        const expiredClaimId =
          randomUUID();
        const expiredClaimAt =
          Date.now() -
          CLAIM_LEASE_MS -
          2_000;

        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "expired claim",
          body:
            "recover through bridge_fetch",
          messageId:
            expiredClaimId,
          now: expiredClaimAt,
        });

        const expiredFirst = bus.claim(
          "claude",
          createConsumerId("claude"),
          1,
          expiredClaimAt,
        );
        assert.equal(
          expiredFirst.length,
          1,
        );

        const beforeClaimHook =
          countEvents(
            dbPath,
            expiredClaimId,
          );
        const claimHook =
          await runHookProcess(
            "user-prompt-submit",
            userProfile,
            {
              hook_event_name:
                "UserPromptSubmit",
              stop_hook_active: false,
            },
          );

        assert.equal(
          claimHook.code,
          0,
        );
        assert.equal(
          claimHook.stderr,
          "",
        );

        const userPromptOutput =
          JSON.parse(
            claimHook.stdout,
          ) as {
            hookSpecificOutput?: Record<
              string,
              unknown
            >;
          };
        assert.deepEqual(
          Object.keys(
            userPromptOutput,
          ),
          ["hookSpecificOutput"],
        );
        assert.deepEqual(
          Object.keys(
            userPromptOutput
              .hookSpecificOutput ?? {},
          ).sort(),
          [
            "additionalContext",
            "hookEventName",
          ],
        );
        assert.equal(
          userPromptOutput
            .hookSpecificOutput
            ?.hookEventName,
          "UserPromptSubmit",
        );
        assert.equal(
          "systemMessage" in
            (userPromptOutput
              .hookSpecificOutput ?? {}),
          false,
        );
        assert.match(
          extractHookNotice(
            claimHook.stdout,
          ),
          /期限切れclaimed=1/,
        );
        assert.equal(
          countEvents(
            dbPath,
            expiredClaimId,
          ),
          beforeClaimHook,
        );

        const claimRetry = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            limit: 1,
            now: Date.now(),
          },
        );

        assert.equal(
          claimRetry.messages.length,
          1,
        );
        assert.notEqual(
          claimRetry.messages[0]!
            .attempt_id,
          expiredFirst[0]!.attempt_id,
        );
        assert.equal(
          countEvents(
            dbPath,
            expiredClaimId,
          ),
          beforeClaimHook + 3,
        );

        bus.ack(
          "claude",
          expiredClaimId,
          claimRetry.messages[0]!
            .attempt_id!,
        );

        const missingProfile = join(
          userProfile,
          "does-not-exist",
        );
        const stopActive =
          await runHookProcess(
            "stop",
            missingProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: true,
            },
          );

        assert.equal(
          stopActive.code,
          0,
        );
        assert.equal(
          stopActive.stdout,
          "",
        );
        assert.equal(
          stopActive.stderr,
          "",
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "14: an empty hook invocation exits zero with no stdout and no row changes",
    async (t) => {
      const {
        userProfile,
        dbPath,
      } = makeProfileDb(t);
      const before =
        databaseSnapshot(dbPath);

      const [stop, userPrompt] =
        await Promise.all([
          runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          ),
          runHookProcess(
            "user-prompt-submit",
            userProfile,
            {
              hook_event_name:
                "UserPromptSubmit",
              stop_hook_active: false,
            },
          ),
        ]);

      for (const result of [
        stop,
        userPrompt,
      ]) {
        assert.equal(result.code, 0);
        assert.equal(
          result.stdout,
          "",
        );
        assert.equal(
          result.stderr,
          "",
        );
      }

      assert.equal(
        databaseSnapshot(dbPath),
        before,
      );
      assert.equal(
        countRows(
          dbPath,
          "messages",
        ),
        0,
      );
      assert.equal(
        countRows(dbPath, "events"),
        0,
      );
    },
  );

  test(
    "15: a non-empty hook is bit-for-bit read-only for message and event rows",
    async (t) => {
      const {
        userProfile,
        dbPath,
      } = makeProfileDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      const stalePresentedAt =
        Date.now() -
        PRESENTED_TTL_MS -
        2_000;
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject:
          "expired presentation",
        body: "hook must not recover",
        messageId: randomUUID(),
        now: stalePresentedAt,
      });
      bus.fetch(
        "claude",
        createConsumerId("claude"),
        {
          limit: 1,
          now: stalePresentedAt,
        },
      );

      const expiredClaimAt =
        Date.now() -
        CLAIM_LEASE_MS -
        2_000;
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "expired claim",
        body: "hook must not recover",
        messageId: randomUUID(),
        now: expiredClaimAt,
      });
      bus.claim(
        "claude",
        createConsumerId("claude"),
        1,
        expiredClaimAt,
      );

      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "read-only proof",
        body: "hook must not claim",
        messageId,
        now: T0,
      });
      bus.close();

      const before =
        databaseSnapshot(dbPath);
      const messageRowsBefore =
        countRows(
          dbPath,
          "messages",
        );
      const eventRowsBefore =
        countRows(dbPath, "events");

      const result =
        await runHookProcess(
          "stop",
          userProfile,
          {
            hook_event_name: "Stop",
            stop_hook_active: false,
          },
        );

      assert.equal(result.code, 0);
      assert.equal(
        result.stderr,
        "",
      );
      const notice = extractHookNotice(
        result.stdout,
      );
      assert.match(notice, /untagged=1/);
      assert.match(
        notice,
        /期限切れclaimed=1/,
      );
      assert.match(
        notice,
        /期限切れpresented=1/,
      );
      assert.equal(
        databaseSnapshot(dbPath),
        before,
      );
      assert.equal(
        countRows(
          dbPath,
          "messages",
        ),
        messageRowsBefore,
      );
      assert.equal(
        countRows(dbPath, "events"),
        eventRowsBefore,
      );

      const verify =
        BridgeBus.open(dbPath);
      try {
        const row =
          verify.readMessage(messageId);
        assert.equal(
          row?.status,
          "stored",
        );
        assert.equal(
          row?.attempt_id,
          null,
        );
        assert.equal(
          row?.attempt_count,
          0,
        );
      } finally {
        verify.close();
      }
    },
  );

  test(
    "16: two concurrent hooks report the same four stored rows without mutation",
    async (t) => {
      const {
        userProfile,
        dbPath,
      } = makeProfileDb(t);
      const bus = BridgeBus.open(dbPath);

      try {
        for (
          let index = 0;
          index < 4;
          index += 1
        ) {
          bus.send({
            fromRole: "codex",
            toRole: "claude",
            subject:
              `parallel ${index}`,
            body: `body ${index}`,
            messageId: randomUUID(),
            now: T0 + index,
          });
        }
      } finally {
        bus.close();
      }

      const before =
        databaseSnapshot(dbPath);

      const [first, second] =
        await Promise.all([
          runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          ),
          runHookProcess(
            "stop",
            userProfile,
            {
              hook_event_name: "Stop",
              stop_hook_active: false,
            },
          ),
        ]);

      for (const result of [
        first,
        second,
      ]) {
        assert.equal(result.code, 0);
        assert.equal(
          result.stderr,
          "",
        );
        const notice =
          extractHookNotice(
            result.stdout,
          );
        assert.match(
          notice,
          /取得可能=4/,
        );
        assert.match(
          notice,
          /untagged=4/,
        );
        assert.match(
          notice,
          /期限切れclaimed=0/,
        );
        assert.match(
          notice,
          /期限切れpresented=0/,
        );
      }

      assert.equal(
        first.stdout,
        second.stdout,
      );
      assert.equal(
        databaseSnapshot(dbPath),
        before,
      );
    },
  );

  test(
    "tool runtime proves send, fetch, and ack invocation through exact event deltas",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();
      const claudeTools =
        new BridgeTools(
          bus,
          "claude",
          createConsumerId("claude"),
        );
      const codexTools =
        new BridgeTools(
          bus,
          "codex",
          createConsumerId("codex"),
        );

      try {
        const baselineEvents =
          countRows(dbPath, "events");

        const send =
          await claudeTools.call(
            "bridge_send",
            {
              subject:
                "real tool call",
              body:
                "count exact deltas",
              message_id: messageId,
            },
          );
        assert.equal(
          send.isError,
          undefined,
        );
        assert.equal(
          countRows(dbPath, "events"),
          baselineEvents + 1,
        );

        const fetch =
          await codexTools.call(
            "bridge_fetch",
            {
              limit: 1,
            },
          );
        assert.equal(
          fetch.isError,
          undefined,
        );
        assert.equal(
          countRows(dbPath, "events"),
          baselineEvents + 3,
        );

        const payload = JSON.parse(
          fetch.content[0]!.text,
        ) as {
          messages: Array<{
            message_id: string;
            attempt_id: string;
          }>;
        };

        assert.equal(
          payload.messages.length,
          1,
        );
        assert.equal(
          payload.messages[0]!
            .message_id,
          messageId,
        );

        const ack =
          await codexTools.call(
            "bridge_ack",
            {
              message_id: messageId,
              attempt_id:
                payload.messages[0]!
                  .attempt_id,
            },
          );
        assert.equal(
          ack.isError,
          undefined,
        );
        assert.equal(
          countRows(dbPath, "events"),
          baselineEvents + 4,
        );

        const beforeInvalid =
          countRows(dbPath, "events");
        const invalid =
          await claudeTools.call(
            "bridge_send",
            {
              subject: 123,
              body: "invalid",
            },
          );
        assert.equal(
          invalid.isError,
          true,
        );
        assert.equal(
          countRows(dbPath, "events"),
          beforeInvalid,
        );

        const sendDefinition =
          TOOL_DEFINITIONS.find(
            (tool) =>
              tool.name ===
              "bridge_send",
          );
        assert.equal(
          sendDefinition?.inputSchema
            .properties.subject.type,
          "string",
        );
        assert.equal(
          sendDefinition?.inputSchema
            .properties.body.type,
          "string",
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "CODEX_THREAD_ID from the environment is never recorded as sender_thread_id",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const codexTools =
        new BridgeTools(
          bus,
          "codex",
          createConsumerId("codex"),
        );
      const envBefore =
        process.env.CODEX_THREAD_ID;
      process.env.CODEX_THREAD_ID =
        "env-thread-should-not-be-recorded";

      try {
        const withoutArg =
          randomUUID();
        const withArg = randomUUID();

        const first =
          await codexTools.call(
            "bridge_send",
            {
              subject:
                "no thread argument",
              body:
                "env must stay out of the ledger",
              message_id:
                withoutArg,
            },
          );
        assert.equal(
          first.isError,
          undefined,
        );

        const second =
          await codexTools.call(
            "bridge_send",
            {
              subject:
                "explicit thread argument",
              body:
                "argument is the only recorded source",
              message_id: withArg,
              thread_id:
                "thread-arg-1",
            },
          );
        assert.equal(
          second.isError,
          undefined,
        );

        const db = new Database(
          dbPath,
          {
            readonly: true,
            fileMustExist: true,
          },
        );
        try {
          const rows = db
            .prepare(
              "SELECT message_id, sender_thread_id FROM messages WHERE message_id IN (?, ?) ORDER BY id",
            )
            .all(
              withoutArg,
              withArg,
            ) as Array<{
            message_id: string;
            sender_thread_id:
              | string
              | null;
          }>;

          assert.equal(
            rows.length,
            2,
          );
          assert.equal(
            rows[0]!
              .sender_thread_id,
            null,
          );
          assert.equal(
            rows[1]!
              .sender_thread_id,
            "thread-arg-1",
          );
        } finally {
          db.close();
        }
      } finally {
        if (envBefore === undefined) {
          delete process.env
            .CODEX_THREAD_ID;
        } else {
          process.env.CODEX_THREAD_ID =
            envBefore;
        }

        bus.close();
      }
    },
  );

  /*
   * v5.2 acceptance coverage.
   */

  test(
    "v5-1: bridge_hello limits tagged visibility and hidden rows cause no claim events",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const sender = new BridgeTools(
        bus,
        "claude",
        createConsumerId("claude"),
      );
      const untagged = new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
      );
      const tagged = new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
      );
      const messageId = randomUUID();

      try {
        await sender.call(
          "bridge_hello",
          { tag: "sender" },
        );
        const sent = await sender.call(
          "bridge_send",
          {
            subject: "tagged",
            body: "visible only to X",
            message_id: messageId,
            to_tag: "X",
          },
        );
        assert.equal(
          sent.isError,
          undefined,
        );

        const eventsAfterSend =
          countRows(dbPath, "events");
        const hidden = await untagged.call(
          "bridge_fetch",
          { limit: 10 },
        );
        const hiddenResult = JSON.parse(
          hidden.content[0]!.text,
        ) as {
          messages: unknown[];
          has_more: boolean;
          unacked_total: number;
        };

        assert.deepEqual(
          hiddenResult,
          {
            messages: [],
            has_more: false,
            unacked_total: 0,
            peek: false,
          },
        );
        assert.equal(
          countRows(dbPath, "events"),
          eventsAfterSend,
        );

        const hello = await tagged.call(
          "bridge_hello",
          { tag: "\n X\u0000 " },
        );
        assert.equal(
          hello.isError,
          undefined,
        );

        const visible = await tagged.call(
          "bridge_fetch",
          { limit: 10 },
        );
        const visibleResult = JSON.parse(
          visible.content[0]!.text,
        ) as {
          messages: Array<{
            message_id: string;
          }>;
        };

        assert.deepEqual(
          visibleResult.messages.map(
            (message) =>
              message.message_id,
          ),
          [messageId],
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "claimed",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "presented",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-2: two processes declaring the same tag still permit only one claim",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "tag race",
        body: "one tagged row",
        messageId,
        toTag: "shared",
        now: T0,
      });
      bus.close();

      const workerA = spawnClaimChild(
        dbPath,
        "shared",
      );
      const workerB = spawnClaimChild(
        dbPath,
        "shared",
      );

      workerA.child.stdin.end("go\n");
      workerB.child.stdin.end("go\n");

      const [resultA, resultB] =
        await Promise.all([
          workerA.result,
          workerB.result,
        ]);

      assert.equal(
        resultA.messages.length +
          resultB.messages.length,
        1,
      );
      assert.equal(
        countEvents(
          dbPath,
          messageId,
          "claimed",
        ),
        1,
      );
    },
  );

  test(
    "v5-3: peek hides other tags and is bit-for-bit read-only",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const xId = randomUUID();
      const yId = randomUUID();
      const roleId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "X",
          body: "X",
          messageId: xId,
          toTag: "X",
          now: T0,
        });
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "Y",
          body: "Y",
          messageId: yId,
          toTag: "Y",
          now: T0,
        });
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "role",
          body: "role",
          messageId: roleId,
          now: T0,
        });

        const before =
          databaseSnapshot(dbPath);
        const result = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            peek: true,
            limit: 10,
            tag: "X",
          },
        );

        assert.deepEqual(
          result.messages.map(
            (message) =>
              message.message_id,
          ),
          [xId, roleId],
        );
        assert.equal(
          result.has_more,
          false,
        );
        assert.equal(
          result.unacked_total,
          2,
        );
        assert.equal(
          databaseSnapshot(dbPath),
          before,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-4: messages, has_more, and unacked_total share one visibility predicate",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "Y only",
          body: "hidden from X",
          messageId: randomUUID(),
          toTag: "Y",
          now: T0,
        });

        const result = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 10,
            now: T0,
            tag: "X",
          },
        );

        assert.deepEqual(
          result.messages,
          [],
        );
        assert.equal(
          result.has_more,
          false,
        );
        assert.equal(
          result.unacked_total,
          0,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-5: presented recovery reaches bounce in the same fetch before a matching tag can reclaim",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject:
            "presented timeout",
          body:
            "must not be re-presented",
          messageId,
          toTag: "X",
          fromTag: "sender",
          now: T0,
        });

        const first = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 1,
            now: T0,
            tag: "X",
          },
        );
        assert.equal(
          first.messages.length,
          1,
        );

        const timeoutAt =
          T0 + TAG_TTL_MS + 1;
        const result = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 1,
            now: timeoutAt,
            tag: "X",
          },
        );

        assert.deepEqual(
          result.messages,
          [],
        );
        assert.equal(
          bus.readMessage(messageId)
            ?.status,
          "bounced",
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "requeued",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "bounced",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-5-A: lease recovery reaches tag timeout in the same fetch",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "lease timeout",
          body:
            "must not be reclaimed",
          messageId,
          toTag: "X",
          fromTag: "sender",
          now: T0,
        });

        const first = bus.claim(
          "codex",
          createConsumerId("codex"),
          1,
          T0,
          "X",
        );
        assert.equal(first.length, 1);

        const timeoutAt =
          T0 + TAG_TTL_MS + 1;
        const result = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 1,
            now: timeoutAt,
            tag: "X",
          },
        );

        assert.deepEqual(
          result.messages,
          [],
        );
        assert.equal(
          bus.readMessage(messageId)
            ?.status,
          "bounced",
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "lease_expired",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            messageId,
            "bounced",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-6 through v5-9: bounce is targeted, minimal, non-chaining, deterministic, fetchable, and ackable",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();
      const originalSubject =
        "SECRET-SUBJECT-7";
      const originalBody =
        "SECRET-BODY-7";
      const destination =
        "SECRET-TARGET-7";

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: originalSubject,
          body: originalBody,
          messageId: originalId,
          toTag: destination,
          fromTag: "sender-session",
          now: T0,
        });

        const bounceAt =
          T0 + TAG_TTL_MS + 1;
        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 10,
            now: bounceAt,
            tag: destination,
          },
        );

        const bounceId =
          deriveBounceMessageId(
            originalId,
          );
        const bounce =
          bus.readMessage(bounceId);

        assert.equal(
          bus.readMessage(originalId)
            ?.status,
          "bounced",
        );
        assert.ok(bounce);
        assert.equal(
          bounce.message_id,
          bounceId,
        );
        assert.match(
          bounceId,
          /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        );
        assert.equal(
          bounce.from_role,
          "codex",
        );
        assert.equal(
          bounce.to_role,
          "claude",
        );
        assert.equal(
          bounce.from_tag,
          null,
        );
        assert.equal(
          bounce.to_tag,
          "sender-session",
        );
        assert.equal(
          bounce.on_timeout,
          "fallback",
        );
        assert.equal(
          bounce.subject,
          BOUNCE_SUBJECT,
        );
        assert.match(
          bounce.body,
          new RegExp(originalId),
        );
        assert.match(
          bounce.body,
          new RegExp(BOUNCE_REASON),
        );
        assert.doesNotMatch(
          bounce.body,
          new RegExp(originalSubject),
        );
        assert.doesNotMatch(
          bounce.body,
          new RegExp(originalBody),
        );
        assert.doesNotMatch(
          bounce.body,
          new RegExp(destination),
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          2,
        );

        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 10,
            now: bounceAt + 1,
            tag: destination,
          },
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          2,
        );

        const delivered = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            limit: 1,
            now: bounceAt + 2,
            tag: "sender-session",
          },
        );
        assert.equal(
          delivered.messages.length,
          1,
        );
        assert.equal(
          delivered.messages[0]!
            .message_id,
          bounceId,
        );

        bus.ack(
          "claude",
          bounceId,
          delivered.messages[0]!
            .attempt_id!,
          bounceAt + 2,
        );
        assert.equal(
          bus.readMessage(bounceId)
            ?.status,
          "acked",
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-8: an unacked bounce falls back role-wide without creating another bounce",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "original",
          body: "original",
          messageId: originalId,
          toTag: "target",
          fromTag: "sender",
          now: T0,
        });

        const firstTimeout =
          T0 + TAG_TTL_MS + 1;
        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now: firstTimeout,
            tag: "target",
          },
        );

        const bounceId =
          deriveBounceMessageId(
            originalId,
          );
        assert.equal(
          bus.readMessage(bounceId)
            ?.on_timeout,
          "fallback",
        );

        const secondTimeout =
          firstTimeout +
          TAG_TTL_MS +
          1;
        const fallbackFetch = bus.fetch(
          "claude",
          createConsumerId("claude"),
          {
            now: secondTimeout,
          },
        );

        assert.equal(
          fallbackFetch.messages[0]
            ?.message_id,
          bounceId,
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          2,
        );
        assert.equal(
          countEvents(
            dbPath,
            bounceId,
            "tag_fallback",
          ),
          1,
        );
        assert.equal(
          countEvents(
            dbPath,
            bounceId,
            "bounced",
          ),
          0,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-10: fallback clears destination routing and becomes claimable without bridge_hello",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "fallback",
          body: "role-wide later",
          messageId,
          toTag: "X",
          onTimeout: "fallback",
          now: T0,
        });

        const fetched = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            limit: 1,
            now:
              T0 +
              TAG_TTL_MS +
              1,
          },
        );

        assert.equal(
          fetched.messages[0]
            ?.message_id,
          messageId,
        );
        const row =
          bus.readMessage(messageId)!;
        assert.equal(row.to_tag, null);
        assert.equal(
          row.on_timeout,
          null,
        );
        assert.equal(
          row.tag_expires_at,
          null,
        );
        assert.equal(
          row.status,
          "presented",
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-11 and v5-11-A: seven-element envelope detects destination, policy, and sender-tag changes",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();
      const senderA = new BridgeTools(
        bus,
        "claude",
        createConsumerId("claude"),
      );
      const senderB = new BridgeTools(
        bus,
        "claude",
        createConsumerId("claude"),
      );

      try {
        await senderA.call(
          "bridge_hello",
          { tag: "A" },
        );
        await senderB.call(
          "bridge_hello",
          { tag: "B" },
        );

        const first = await senderA.call(
          "bridge_send",
          {
            subject: "same",
            body: "same",
            message_id: messageId,
            to_tag: "X",
            on_timeout: "bounce",
          },
        );
        assert.equal(
          first.isError,
          undefined,
        );

        const repeated =
          await senderA.call(
            "bridge_send",
            {
              subject: "same",
              body: "same",
              message_id: messageId,
              to_tag: "X",
              on_timeout: "bounce",
            },
          );
        assert.equal(
          repeated.isError,
          undefined,
        );
        assert.match(
          repeated.content[0]!.text,
          /idempotent/,
        );

        const before =
          messageSnapshot(
            dbPath,
            messageId,
          );

        const changedDestination =
          await senderA.call(
            "bridge_send",
            {
              subject: "same",
              body: "same",
              message_id: messageId,
              to_tag: "Y",
              on_timeout: "bounce",
            },
          );
        assert.equal(
          changedDestination.isError,
          true,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            messageId,
          ),
          before,
        );

        const changedPolicy =
          await senderA.call(
            "bridge_send",
            {
              subject: "same",
              body: "same",
              message_id: messageId,
              to_tag: "X",
              on_timeout:
                "fallback",
            },
          );
        assert.equal(
          changedPolicy.isError,
          true,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            messageId,
          ),
          before,
        );

        const changedSender =
          await senderB.call(
            "bridge_send",
            {
              subject: "same",
              body: "same",
              message_id: messageId,
              to_tag: "X",
              on_timeout: "bounce",
            },
          );
        assert.equal(
          changedSender.isError,
          true,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            messageId,
          ),
          before,
        );

        const invalidPolicy =
          await senderA.call(
            "bridge_send",
            {
              subject: "invalid",
              body: "invalid",
              on_timeout: "bounce",
            },
          );
        assert.equal(
          invalidPolicy.isError,
          true,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-11-B: bounce update, notification insert, and both event writes roll back atomically",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "atomic",
          body: "atomic",
          messageId: originalId,
          toTag: "target",
          fromTag: "sender",
          now: T0,
        });

        const inject =
          new Database(dbPath);
        try {
          inject.exec(`
CREATE TRIGGER fail_bounce_insert
BEFORE INSERT ON messages
WHEN NEW.subject = '${BOUNCE_SUBJECT}'
BEGIN
  SELECT RAISE(
    ABORT,
    'injected bounce insertion failure'
  );
END;
`);
        } finally {
          inject.close();
        }

        const before =
          databaseSnapshot(dbPath);
        assert.throws(
          () =>
            bus.fetch(
              "codex",
              createConsumerId(
                "codex",
              ),
              {
                now:
                  T0 +
                  TAG_TTL_MS +
                  1,
                tag: "target",
              },
            ),
          /injected bounce insertion failure/,
        );
        assert.equal(
          databaseSnapshot(dbPath),
          before,
        );
        assert.equal(
          bus.readMessage(originalId)
            ?.status,
          "stored",
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          1,
        );

        const removeTrigger =
          new Database(dbPath);
        try {
          removeTrigger.exec(
            "DROP TRIGGER fail_bounce_insert",
          );
        } finally {
          removeTrigger.close();
        }

        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now:
              T0 +
              TAG_TTL_MS +
              2,
            tag: "target",
          },
        );

        assert.equal(
          bus.readMessage(originalId)
            ?.status,
          "bounced",
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          2,
        );
        assert.equal(
          countEvents(
            dbPath,
            originalId,
            "bounced",
          ),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-11-B2: a failure on the bounce event write rolls back the update and the notification",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "atomic events",
          body: "atomic events",
          messageId: originalId,
          toTag: "target",
          fromTag: "sender",
          now: T0,
        });

        // The sibling test injects on the notification INSERT. This one
        // injects on the events write, so an implementation that commits
        // the row update and the notification before appending events is
        // caught as well.
        const inject = new Database(dbPath);
        try {
          inject.exec(`
CREATE TRIGGER fail_bounce_event
BEFORE INSERT ON events
WHEN NEW.event = 'bounced'
BEGIN
  SELECT RAISE(
    ABORT,
    'injected bounce event failure'
  );
END;
`);
        } finally {
          inject.close();
        }

        const before = databaseSnapshot(dbPath);

        assert.throws(
          () =>
            bus.fetch(
              "codex",
              createConsumerId("codex"),
              {
                now: T0 + TAG_TTL_MS + 1,
                tag: "target",
              },
            ),
          /injected bounce event failure/,
        );

        assert.equal(databaseSnapshot(dbPath), before);
        assert.equal(
          bus.readMessage(originalId)?.status,
          "stored",
        );
        assert.equal(countRows(dbPath, "messages"), 1);

        const removeTrigger = new Database(dbPath);
        try {
          removeTrigger.exec("DROP TRIGGER fail_bounce_event");
        } finally {
          removeTrigger.close();
        }

        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now: T0 + TAG_TTL_MS + 2,
            tag: "target",
          },
        );

        assert.equal(
          bus.readMessage(originalId)?.status,
          "bounced",
        );
        assert.equal(countRows(dbPath, "messages"), 2);
        assert.equal(
          countEvents(dbPath, originalId, "bounced"),
          1,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-11-C: a bounce to an untagged sender is normalized as role-wide",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject:
            "untagged sender",
          body: "untagged sender",
          messageId: originalId,
          toTag: "target",
          now: T0,
        });

        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now:
              T0 +
              TAG_TTL_MS +
              1,
            tag: "target",
          },
        );

        const bounce =
          bus.readMessage(
            deriveBounceMessageId(
              originalId,
            ),
          )!;

        assert.equal(
          bounce.to_tag,
          null,
        );
        assert.equal(
          bounce.on_timeout,
          null,
        );
        assert.equal(
          bounce.tag_expires_at,
          null,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-12: bounced is terminal under recovery, stale ack, and idempotent resend",
    (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const originalId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "terminal bounce",
          body: "terminal bounce",
          messageId: originalId,
          toTag: "target",
          fromTag: "sender",
          onTimeout: "bounce",
          now: T0,
        });

        const first = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now: T0,
            tag: "target",
          },
        );
        const oldAttempt =
          first.messages[0]!.attempt_id!;

        bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now:
              T0 +
              TAG_TTL_MS +
              1,
            tag: "target",
          },
        );

        const before =
          messageSnapshot(
            dbPath,
            originalId,
          );
        bus.recover(
          "codex",
          T0 +
            TAG_TTL_MS +
            PRESENTED_TTL_MS +
            10,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            originalId,
          ),
          before,
        );

        assert.throws(
          () =>
            bus.ack(
              "codex",
              originalId,
              oldAttempt,
              T0 +
                TAG_TTL_MS +
                2,
            ),
          BridgeTransitionError,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            originalId,
          ),
          before,
        );

        const repeated = bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "terminal bounce",
          body: "terminal bounce",
          messageId: originalId,
          toTag: "target",
          fromTag: "sender",
          onTimeout: "bounce",
          now:
            T0 +
            TAG_TTL_MS +
            3,
        });
        assert.equal(
          repeated.idempotent,
          true,
        );
        assert.equal(
          messageSnapshot(
            dbPath,
            originalId,
          ),
          before,
        );
      } finally {
        bus.close();
      }
    },
  );

  test(
    "v5-13: bridge-init --migrate preserves rows and recomputes the seven-element envelope",
    async (t) => {
      const {
        userProfile,
        dbPath,
      } = makeLegacyProfileDb(t);
      const messageId = randomUUID();
      const subject =
        "legacy subject";
      const body = "legacy body";

      insertLegacyMessage(dbPath, {
        messageId,
        subject,
        body,
      });

      assert.doesNotThrow(() =>
        openAsLegacyServer(dbPath),
      );

      const result =
        await runBridgeInitProcess(
          userProfile,
          ["--migrate"],
        );

      assert.equal(
        result.code,
        0,
        result.stderr,
      );
      assert.equal(result.stdout, "");
      assert.match(
        result.stderr,
        /schema_version=4\.0/,
      );

      const db = new Database(dbPath, {
        readonly: true,
        fileMustExist: true,
      });
      try {
        const schema = db
          .prepare(
            "SELECT v FROM meta WHERE k = 'schema_version'",
          )
          .get() as { v: string };
        const row = db
          .prepare(
            "SELECT * FROM messages WHERE message_id = ?",
          )
          .get(messageId) as {
          subject: string;
          body: string;
          envelope_sha256: string;
          to_tag: string | null;
          from_tag: string | null;
          on_timeout: string | null;
          tag_expires_at: number | null;
        };

        assert.equal(
          schema.v,
          SCHEMA_VERSION,
        );
        assert.equal(
          row.subject,
          subject,
        );
        assert.equal(row.body, body);
        assert.equal(row.to_tag, null);
        assert.equal(
          row.from_tag,
          null,
        );
        assert.equal(
          row.on_timeout,
          null,
        );
        assert.equal(
          row.tag_expires_at,
          null,
        );
        assert.equal(
          row.envelope_sha256,
          computeEnvelopeHash(
            "claude",
            "codex",
            subject,
            body,
            null,
            null,
            null,
          ),
        );
        assert.equal(
          countRows(
            dbPath,
            "messages",
          ),
          1,
        );
        assert.equal(
          countRows(dbPath, "events"),
          1,
        );
      } finally {
        db.close();
      }

      const current =
        BridgeBus.open(dbPath);
      current.close();

      assert.throws(
        () =>
          openAsLegacyServer(
            dbPath,
          ),
        /unsupported schema_version 4\.0/,
      );
    },
  );

  test(
    "v5-13-A: a post-DDL migration failure restores schema, rows, hashes, and version 3.2",
    (t) => {
      const {
        dbPath,
      } = makeLegacyProfileDb(t);
      const messageId = randomUUID();

      insertLegacyMessage(dbPath, {
        messageId,
        subject: "rollback",
        body: "rollback",
      });

      const before =
        legacyDatabaseSnapshot(dbPath);

      assert.throws(
        () =>
          migrateBridgeDatabaseAtPath(
            dbPath,
            {
              failAfterDestructiveDdl:
                true,
            },
          ),
        /injected migration failure after destructive DDL/,
      );

      assert.equal(
        legacyDatabaseSnapshot(dbPath),
        before,
      );
      assert.doesNotThrow(() =>
        openAsLegacyServer(dbPath),
      );
    },
  );

  test(
    "v6-1: a live tagged row is counted as addressed elsewhere, not as fetchable",
    async (t) => {
      const profile = makeProfileDb(t);
      const bus = BridgeBus.open(profile.dbPath);

      try {
        // Addressed to a session that this hook's session has not declared.
        // Counting it as fetchable makes every untagged session chase mail
        // it cannot see, which is what happened in production.
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "for another session",
          body: "invisible to untagged sessions",
          messageId: randomUUID(),
          toTag: "apps-hub",
          fromTag: "sender",
          now: Date.now(),
        });
      } finally {
        bus.close();
      }

      const before = databaseSnapshot(profile.dbPath);
      const result = await runHookProcess(
        "stop",
        profile.userProfile,
        {
          hook_event_name: "Stop",
          stop_hook_active: false,
        },
      );

      assert.equal(result.code, 0);
      assert.equal(result.stderr, "");

      const notice = extractHookNotice(result.stdout);
      assert.match(notice, /取得可能=0/);
      assert.match(notice, /他セッション宛=1/);
      assert.equal(databaseSnapshot(profile.dbPath), before);
    },
  );

  test(
    "v6-3: untagged and live tagged rows are reported in their own buckets",
    async (t) => {
      const profile = makeProfileDb(t);
      const bus = BridgeBus.open(profile.dbPath);

      try {
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "for anyone",
          body: "untagged",
          messageId: randomUUID(),
          now: Date.now(),
        });
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "for one session",
          body: "tagged",
          messageId: randomUUID(),
          toTag: "apps-hub",
          fromTag: "sender",
          now: Date.now(),
        });
      } finally {
        bus.close();
      }

      const result = await runHookProcess(
        "stop",
        profile.userProfile,
        {
          hook_event_name: "Stop",
          stop_hook_active: false,
        },
      );

      // Both rows exist; neither bucket may swallow the other.
      const notice = extractHookNotice(result.stdout);
      assert.match(notice, /取得可能=1/);
      assert.match(notice, /他セッション宛=1/);
      assert.match(notice, /untagged=1/);
    },
  );

  test(
    "v6-2: an expired tagged row stays fetchable so recovery keeps an executor",
    async (t) => {
      const profile = makeProfileDb(t);
      const bus = BridgeBus.open(profile.dbPath);
      const expiredAt = Date.now() - TAG_TTL_MS - 2_000;

      try {
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: "expired tag executor",
          body: "any session must be told to fetch",
          messageId: randomUUID(),
          toTag: "offline",
          fromTag: "sender",
          now: expiredAt,
        });
      } finally {
        bus.close();
      }

      const result = await runHookProcess(
        "stop",
        profile.userProfile,
        {
          hook_event_name: "Stop",
          stop_hook_active: false,
        },
      );

      const notice = extractHookNotice(result.stdout);
      assert.match(notice, /取得可能=1/);
      assert.match(notice, /他セッション宛=0/);
    },
  );

  test(
    "v5-14: untagged delivery remains compatible and hook notices tag-expired rows without mutation",
    async (t) => {
      const { dbPath } = makeDb(t);
      const bus = BridgeBus.open(dbPath);
      const messageId = randomUUID();

      try {
        bus.send({
          fromRole: "claude",
          toRole: "codex",
          subject: "legacy flow",
          body: "legacy flow",
          messageId,
          now: T0,
        });

        const fetched = bus.fetch(
          "codex",
          createConsumerId("codex"),
          {
            now: T0,
          },
        );
        assert.equal(
          fetched.messages[0]
            ?.message_id,
          messageId,
        );
        bus.ack(
          "codex",
          messageId,
          fetched.messages[0]!
            .attempt_id!,
          T0,
        );
        assert.equal(
          bus.status(messageId).message
            .status,
          "acked",
        );
      } finally {
        bus.close();
      }

      const profile =
        makeProfileDb(t);
      const taggedBus = BridgeBus.open(
        profile.dbPath,
      );
      const expiredId = randomUUID();
      const expiredAt =
        Date.now() -
        TAG_TTL_MS -
        1;

      try {
        taggedBus.send({
          fromRole: "codex",
          toRole: "claude",
          subject:
            "expired tag executor",
          body:
            "hook must request fetch",
          messageId: expiredId,
          toTag: "offline",
          fromTag: "sender",
          now: expiredAt,
        });
      } finally {
        taggedBus.close();
      }

      const before = databaseSnapshot(
        profile.dbPath,
      );
      const notice =
        await runHookProcess(
          "stop",
          profile.userProfile,
          {
            hook_event_name: "Stop",
            stop_hook_active: false,
          },
        );

      assert.equal(notice.code, 0);
      assert.equal(
        notice.stderr,
        "",
      );
      assert.match(
        extractHookNotice(
          notice.stdout,
        ),
        /untagged=0/,
      );
      assert.match(
        extractHookNotice(
          notice.stdout,
        ),
        /期限切れtag=1/,
      );
      assert.equal(
        databaseSnapshot(
          profile.dbPath,
        ),
        before,
      );
    },
  );
}
