import assert from "node:assert/strict";
import {
  execFileSync,
  spawn,
} from "node:child_process";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
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
  lostQuerySql,
  migrateBridgeDatabaseAtPath,
  sha256,
} from "../src/db.js";
import {
  BridgeTools,
  TOOL_DEFINITIONS,
} from "../src/tools.js";
import {
  checkControlCharacters,
  checkReferences,
  checkTranscripts,
  isSkip,
  runDocCheck,
} from "../src/doc-check.js";
import {
  formatAge,
  formatBacklog,
  formatSubject,
  formatUndelivered,
} from "../src/bridge-sweep.js";

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
const SWEEP_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-sweep.ts",
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

  function fetchJson(text: string): string {
    const start = text.indexOf("{");

    if (start < 0) {
      throw new Error(
        "bridge_fetch did not return JSON",
      );
    }

    return text.slice(start);
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
            bounced: 0,
            fallbackDemoted: 0,
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
            undefined,
            bus.readMessage(
              messageId,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              messageId,
            )!.consumer!,
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
              createConsumerId("codex"),
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
            bus.readMessage(
              messageId,
            )!.consumer!,
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
            bus.readMessage(
              messageId,
            )!.consumer!,
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
            bus.readMessage(
              messageId,
            )!.consumer!,
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
            bus.readMessage(
              good,
            )!.consumer!,
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
            bus.readMessage(
              messageId,
            )!.consumer!,
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
        /pid=\d+.*root_id=.*schema_version=4\.0 require_tag_at_start=none strict_addressing_at_start=none/,
      );
      assert.match(
        startedCodex.stderr,
        /pid=\d+.*root_id=.*schema_version=4\.0 require_tag_at_start=none strict_addressing_at_start=none/,
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
          fetchJson(
            fetched.content[0]!.text,
          ),
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
            bus.readMessage(
              message.message_id,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              storedId,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              freshPresentedId,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              stalePresentedId,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              stalePresentedId,
            )!.consumer!,
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
            undefined,
            bus.readMessage(
              expiredClaimId,
            )!.consumer!,
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
          fetchJson(
            fetch.content[0]!.text,
          ),
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
          fetchJson(
            hidden.content[0]!.text,
          ),
        ) as {
          messages: unknown[];
          has_more: boolean;
          unacked_total: number;
        };

        assert.deepEqual(
          hiddenResult,
          {
            declared_tag: null,
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
            /*
             * The sends are at T0, so peeking at the wall clock puts the
             * tags an hour past their TTL and the test would be reading
             * expired rows while claiming to be about visibility.
             */
            now: T0,
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
            bus.readMessage(
              bounceId,
            )!.consumer!,
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
              createConsumerId("codex"),
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
            bus.readMessage(
              messageId,
            )!.consumer!,
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

  test(
    "v7-1: sweep bounces expired tagged messages for both roles without fetch",
    async () => {
      const fs =
        await import("node:fs/promises");
      const os = await import("node:os");
      const { default: Database } =
        await import("better-sqlite3");
      const userProfile =
        await fs.mkdtemp(
          join(
            os.tmpdir(),
            "agent-bridge-v7-1-",
          ),
        );
      const dbPath = join(
        userProfile,
        ".claude",
        "data",
        "agent-bridge",
        "bridge.db",
      );
      const claudeSubject =
        "v7-1 expired to claude";
      const codexSubject =
        "v7-1 expired to codex";

      try {
        await fs.mkdir(
          join(
            userProfile,
            ".claude",
            "data",
            "agent-bridge",
          ),
          { recursive: true },
        );
        initializeBridgeDatabaseAtPath(
          dbPath,
        );

        const bus = BridgeBus.open(dbPath);
        try {
          bus.send({
            fromRole: "codex",
            fromTag:
              "codex-v7-1-source",
            toRole: "claude",
            toTag:
              "claude-v7-1-target",
            onTimeout: "bounce",
            subject: claudeSubject,
            body: "expired",
            now: T0,
          });
          bus.send({
            fromRole: "claude",
            fromTag:
              "claude-v7-1-source",
            toRole: "codex",
            toTag:
              "codex-v7-1-target",
            onTimeout: "bounce",
            subject: codexSubject,
            body: "expired",
            now: T0,
          });
        } finally {
          bus.close();
        }

        const seedDb = new Database(
          dbPath,
        );
        let originalIds:
          readonly string[];
        try {
          const rows = seedDb
            .prepare(
              `
                SELECT
                  message_id AS messageId
                FROM messages
                WHERE subject IN (?, ?)
                ORDER BY subject
              `,
            )
            .all(
              claudeSubject,
              codexSubject,
            ) as Array<{
              messageId: string;
            }>;

          assert.equal(rows.length, 2);
          originalIds = rows.map(
            (row) => row.messageId,
          );

          seedDb
            .prepare(
              `
                UPDATE messages
                SET tag_expires_at = ?
                WHERE message_id IN (?, ?)
              `,
            )
            .run(
              T0 - 1,
              originalIds[0],
              originalIds[1],
            );
        } finally {
          seedDb.close();
        }

        const result =
          await runTypeScriptProcess(
            SWEEP_ENTRY,
            [],
            userProfile,
          );

        assert.equal(
          result.code,
          0,
          result.stderr,
        );
        assert.equal(result.stdout, "");
        const stderrLines = result.stderr
          .trim()
          .split(/\r?\n/);

        assert.equal(
          stderrLines[0],
          `agent-bridge sweep db=${JSON.stringify(
            dbPath,
          )} claude=lease:0,requeued:0,bounced:1,fallback:0,untagged:0,oldest:- codex=lease:0,requeued:0,bounced:1,fallback:0,untagged:0,oldest:-`,
        );

        /*
         * The counts line says how many bounced. These say which, which
         * is the part a person can act on.
         */
        for (const role of ["claude", "codex"]) {
          assert.ok(
            stderrLines.some((line) =>
              line.startsWith(
                `agent-bridge ${role} 1 undelivered not yet reported`,
              ),
            ),
            result.stderr,
          );
        }

        const verifyDb = new Database(
          dbPath,
        );
        try {
          const originals = verifyDb
            .prepare(
              `
                SELECT status
                FROM messages
                WHERE message_id IN (?, ?)
                ORDER BY message_id
              `,
            )
            .all(
              originalIds[0],
              originalIds[1],
            ) as Array<{
              status: string;
            }>;

          assert.deepEqual(
            originals.map(
              (row) => row.status,
            ),
            ["bounced", "bounced"],
          );

          const bounceRows = verifyDb
            .prepare(
              `
                SELECT
                  to_role AS toRole,
                  to_tag AS toTag,
                  status
                FROM messages
                WHERE message_id NOT IN (?, ?)
                ORDER BY to_role, to_tag
              `,
            )
            .all(
              originalIds[0],
              originalIds[1],
            ) as Array<{
              toRole: string;
              toTag: string | null;
              status: string;
            }>;

          assert.deepEqual(
            bounceRows,
            [
              {
                toRole: "claude",
                toTag:
                  "claude-v7-1-source",
                status: "stored",
              },
              {
                toRole: "codex",
                toTag:
                  "codex-v7-1-source",
                status: "stored",
              },
            ],
          );
        } finally {
          verifyDb.close();
        }
      } finally {
        await fs.rm(
          userProfile,
          {
            recursive: true,
            force: true,
          },
        );
      }
    },
  );

  test(
    "v7-2: sweep does not claim or present live messages",
    async () => {
      const fs =
        await import("node:fs/promises");
      const os = await import("node:os");
      const { default: Database } =
        await import("better-sqlite3");
      const userProfile =
        await fs.mkdtemp(
          join(
            os.tmpdir(),
            "agent-bridge-v7-2-",
          ),
        );
      const dbPath = join(
        userProfile,
        ".claude",
        "data",
        "agent-bridge",
        "bridge.db",
      );
      const untaggedSubject =
        "v7-2 untagged sentinel";
      const taggedSubject =
        "v7-2 tagged sentinel";

      try {
        await fs.mkdir(
          join(
            userProfile,
            ".claude",
            "data",
            "agent-bridge",
          ),
          { recursive: true },
        );
        initializeBridgeDatabaseAtPath(
          dbPath,
        );

        const now = Date.now();
        const bus = BridgeBus.open(dbPath);
        try {
          bus.send({
            fromRole: "claude",
            toRole: "codex",
            subject: untaggedSubject,
            body: "live",
            now,
          });
          bus.send({
            fromRole: "codex",
            fromTag:
              "codex-v7-2-source",
            toRole: "claude",
            toTag:
              "claude-v7-2-target",
            onTimeout: "bounce",
            subject: taggedSubject,
            body: "live",
            now,
          });
        } finally {
          bus.close();
        }

        const beforeDb = new Database(
          dbPath,
        );
        let beforeMessages:
          readonly Record<
            string,
            unknown
          >[];
        let beforeDeliveryEvents: number;
        try {
          beforeMessages = beforeDb
            .prepare(
              `
                SELECT *
                FROM messages
                WHERE subject IN (?, ?)
                ORDER BY subject
              `,
            )
            .all(
              untaggedSubject,
              taggedSubject,
            ) as Array<
              Record<string, unknown>
            >;

          beforeDeliveryEvents = (
            beforeDb
              .prepare(
                `
                  SELECT COUNT(*) AS count
                  FROM events
                  WHERE event
                    IN ('claimed', 'presented')
                `,
              )
              .get() as {
                count: number;
              }
          ).count;
        } finally {
          beforeDb.close();
        }

        assert.equal(
          beforeMessages.length,
          2,
        );

        const result =
          await runTypeScriptProcess(
            SWEEP_ENTRY,
            [],
            userProfile,
          );

        assert.equal(
          result.code,
          0,
          result.stderr,
        );
        assert.equal(result.stdout, "");
        assert.equal(
          result.stderr.trim(),
          `agent-bridge sweep db=${JSON.stringify(
            dbPath,
          )} claude=lease:0,requeued:0,bounced:0,fallback:0,untagged:0,oldest:- codex=lease:0,requeued:0,bounced:0,fallback:0,untagged:1,oldest:0h`,
        );

        const afterDb = new Database(
          dbPath,
        );
        try {
          const afterMessages = afterDb
            .prepare(
              `
                SELECT *
                FROM messages
                WHERE subject IN (?, ?)
                ORDER BY subject
              `,
            )
            .all(
              untaggedSubject,
              taggedSubject,
            ) as Array<
              Record<string, unknown>
            >;

          const afterDeliveryEvents = (
            afterDb
              .prepare(
                `
                  SELECT COUNT(*) AS count
                  FROM events
                  WHERE event
                    IN ('claimed', 'presented')
                `,
              )
              .get() as {
                count: number;
              }
          ).count;

          assert.deepEqual(
            afterMessages,
            beforeMessages,
          );
          assert.equal(
            afterDeliveryEvents,
            beforeDeliveryEvents,
          );
        } finally {
          afterDb.close();
        }
      } finally {
        await fs.rm(
          userProfile,
          {
            recursive: true,
            force: true,
          },
        );
      }
    },
  );

  type V8FetchJson = {
    declared_tag: string | null;
    next_cursor?: number | null;
    messages: Array<{
      message_id: string;
      attempt_id: string | null;
      subject: string;
      to_tag: string | null;
      from_tag: string | null;
      body_bytes: number;
      body?: string;
      redelivery: boolean;
    }>;
    has_more: boolean;
    unacked_total: number;
    peek: boolean;
  };

  function parseV8Fetch(result: {
    content: Array<{
      type: string;
      text?: string;
    }>;
  }): V8FetchJson {
    const content = result.content[0];

    if (
      content?.type !== "text" ||
      content.text === undefined
    ) {
      throw new Error(
        "bridge_fetch did not return text",
      );
    }

    const jsonStart =
      content.text.indexOf("{");

    if (jsonStart < 0) {
      throw new Error(
        "bridge_fetch did not return JSON",
      );
    }

    return JSON.parse(
      content.text.slice(jsonStart),
    ) as V8FetchJson;
  }

  function createV8Tools(t: TestContext) {
    const directory = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-v8-",
      ),
    );
    const dbPath = join(
      directory,
      "bridge.db",
    );

    initializeBridgeDatabaseAtPath(dbPath);

    const bus = BridgeBus.open(dbPath);

    t.after(() => {
      bus.close();
      rmSync(directory, {
        recursive: true,
        force: true,
      });
    });

    return {
      bus,
      claude: new BridgeTools(
        bus,
        "claude",
        createConsumerId("claude"),
        { tag: null },
      ),
      codex: new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
        { tag: null },
      ),
    };
  }

  test("v8-1 peek omits body", async (t) => {
    const { claude, codex } =
      createV8Tools(t);

    const subject = "v8-1 subject";
    const body = "peek では返さない本文";

    await claude.call("bridge_send", {
      subject,
      body,
      thread_id: "v8-1",
    });

    const result = parseV8Fetch(
      await codex.call("bridge_fetch", {
        limit: 3,
        peek: true,
      }),
    );

    assert.equal(result.messages.length, 1);

    const message = result.messages[0];
    assert.ok(message);
    assert.equal(message.subject, subject);
    assert.equal(
      message.body_bytes,
      Buffer.byteLength(body, "utf8"),
    );
    assert.equal(
      Object.prototype.hasOwnProperty.call(
        message,
        "body",
      ),
      false,
    );
  });

  test("v8-2 claim fetch preserves body", async (t) => {
    const { claude, codex } =
      createV8Tools(t);

    const body = "配達される本文";

    await claude.call("bridge_send", {
      subject: "v8-2 subject",
      body,
      thread_id: "v8-2",
    });

    const result = parseV8Fetch(
      await codex.call("bridge_fetch", {
        limit: 3,
        peek: false,
      }),
    );

    assert.equal(result.messages.length, 1);

    const message = result.messages[0];
    assert.ok(message);
    assert.equal(
      Object.prototype.hasOwnProperty.call(
        message,
        "body",
      ),
      true,
    );
    assert.equal(message.body, body);
  });

  test("v8-3 fetch returns declared tag", async (t) => {
    const { claude, codex } =
      createV8Tools(t);

    const beforeHello = parseV8Fetch(
      await codex.call("bridge_fetch", {
        limit: 3,
        peek: true,
      }),
    );

    assert.equal(beforeHello.declared_tag, null);

    await codex.call("bridge_hello", {
      tag: "winsmux-lane",
    });

    const afterHello = parseV8Fetch(
      await codex.call("bridge_fetch", {
        limit: 3,
        peek: true,
      }),
    );

    assert.equal(
      afterHello.declared_tag,
      "winsmux-lane",
    );
  });

  test("v8-4 fetch returns routing metadata", async (t) => {
    const { claude, codex } =
      createV8Tools(t);

    const body = "日本語を含む routing 本文";

    await claude.call("bridge_hello", {
      tag: "apps-hub",
    });
    await codex.call("bridge_hello", {
      tag: "winsmux-lane",
    });
    await claude.call("bridge_send", {
      subject: "v8-4 subject",
      body,
      thread_id: "v8-4",
      to_tag: "winsmux-lane",
    });

    const result = parseV8Fetch(
      await codex.call("bridge_fetch", {
        limit: 3,
        peek: true,
      }),
    );

    assert.equal(result.messages.length, 1);

    const message = result.messages[0];
    assert.ok(message);
    assert.equal(message.to_tag, "winsmux-lane");
    assert.equal(message.from_tag, "apps-hub");
    assert.equal(
      message.body_bytes,
      Buffer.byteLength(body, "utf8"),
    );
  });

  function createV9Tools(t: TestContext) {
    const directory = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-v9-",
      ),
    );
    const dbPath = join(
      directory,
      "bridge.db",
    );

    initializeBridgeDatabaseAtPath(dbPath);

    const bus = BridgeBus.open(dbPath);

    t.after(() => {
      bus.close();
      rmSync(directory, {
        recursive: true,
        force: true,
      });
    });

    return {
      bus,
      claude: new BridgeTools(
        bus,
        "claude",
        createConsumerId("claude"),
        { tag: null },
      ),
      codex: new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
        { tag: null },
      ),
      taggedCodex: new BridgeTools(
        bus,
        "codex",
        createConsumerId("codex"),
        { tag: null },
      ),
    };
  }

  function v9Json(result: {
    content: Array<{
      type: string;
      text?: string;
    }>;
  }): V8FetchJson {
    const content = result.content[0];

    if (
      content?.type !== "text" ||
      content.text === undefined
    ) {
      throw new Error(
        "bridge_fetch did not return text",
      );
    }

    return JSON.parse(
      fetchJson(content.text),
    ) as V8FetchJson;
  }

  test("v9-1: a message_id fetch takes that message and leaves the rest stored", async (t) => {
    const { bus, claude, codex } =
      createV9Tools(t);

    const first =
      "11111111-1111-4111-8111-1111111119a1";
    const second =
      "22222222-2222-4222-8222-2222222229a1";

    await claude.call("bridge_send", {
      subject: "v9-1 first",
      body: "最初の便",
      message_id: first,
    });
    await claude.call("bridge_send", {
      subject: "v9-1 second",
      body: "二番目の便",
      message_id: second,
    });

    const fetched = v9Json(
      await codex.call("bridge_fetch", {
        message_id: second,
        limit: 2,
      }),
    );

    assert.equal(fetched.messages.length, 1);
    assert.equal(
      fetched.messages[0]?.message_id,
      second,
    );
    assert.equal(
      fetched.messages[0]?.body,
      "二番目の便",
    );

    assert.equal(
      bus.status(first).message.status,
      "stored",
    );
    assert.equal(
      bus.status(second).message.status,
      "presented",
    );
  });

  test("v9-2: knowing the id of a tagged message is not enough to take it", async (t) => {
    const { bus, claude, codex, taggedCodex } =
      createV9Tools(t);

    const target =
      "33333333-3333-4333-8333-3333333339a2";

    await claude.call("bridge_send", {
      subject: "v9-2 tagged",
      body: "レーン宛の本文",
      message_id: target,
      to_tag: "lane-x",
    });

    const withoutTag = v9Json(
      await codex.call("bridge_fetch", {
        message_id: target,
      }),
    );

    assert.equal(
      withoutTag.messages.length,
      0,
    );
    assert.equal(
      bus.status(target).message.status,
      "stored",
    );
    assert.equal(
      bus.readMessage(target)?.to_tag,
      "lane-x",
    );

    await taggedCodex.call("bridge_hello", {
      tag: "lane-x",
    });

    const withTag = v9Json(
      await taggedCodex.call(
        "bridge_fetch",
        { message_id: target },
      ),
    );

    assert.equal(withTag.messages.length, 1);
    assert.equal(
      withTag.messages[0]?.message_id,
      target,
    );
    assert.equal(
      withTag.messages[0]?.body,
      "レーン宛の本文",
    );
  });

  test("v9-3: an invisible message and a missing one answer the same way", async (t) => {
    const { claude, codex } =
      createV9Tools(t);

    const hidden =
      "44444444-4444-4444-8444-4444444449a3";
    const absent =
      "55555555-5555-4555-8555-5555555559a3";

    await claude.call("bridge_send", {
      subject: "v9-3 tagged",
      body: "見えない本文",
      message_id: hidden,
      to_tag: "lane-y",
    });

    const hiddenResult = await codex.call(
      "bridge_fetch",
      { message_id: hidden },
    );
    const absentResult = await codex.call(
      "bridge_fetch",
      { message_id: absent },
    );

    assert.deepEqual(
      hiddenResult,
      absentResult,
    );
  });

  test("v9-4: a message_id fetch still runs recovery", async (t) => {
    const { bus, claude } = createV9Tools(t);

    const stale =
      "66666666-6666-4666-8666-6666666669a4";
    const target =
      "77777777-7777-4777-8777-7777777779a4";

    bus.send({
      fromRole: "claude",
      toRole: "codex",
      subject: "v9-4 stale",
      body: "lease が切れる便",
      messageId: stale,
      now: T0,
    });
    bus.send({
      fromRole: "claude",
      toRole: "codex",
      subject: "v9-4 target",
      body: "単発で取る便",
      messageId: target,
      now: T0,
    });

    bus.claim(
      "codex",
      createConsumerId("codex"),
      1,
      T0,
      null,
    );

    assert.equal(
      bus.status(stale).message.status,
      "claimed",
    );

    bus.fetch(
      "codex",
      createConsumerId("codex"),
      {
        messageId: target,
        now: T0 + CLAIM_LEASE_MS + 1,
      },
    );

    assert.equal(
      bus.status(stale).message.status,
      "stored",
    );
    assert.equal(
      bus.status(target).message.status,
      "presented",
    );

    void claude;
  });

  function createV10Bus(t: TestContext): {
    bus: BridgeBus;
    dbPath: string;
  } {
    const directory = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-v10-",
      ),
    );
    const dbPath = join(
      directory,
      "bridge.db",
    );

    initializeBridgeDatabaseAtPath(dbPath);

    const bus = BridgeBus.open(dbPath);

    t.after(() => {
      bus.close();
      rmSync(directory, {
        recursive: true,
        force: true,
      });
    });

    return { bus, dbPath };
  }

  function countMessages(
    dbPath: string,
  ): number {
    const db = new Database(dbPath, {
      readonly: true,
    });

    try {
      return (
        db
          .prepare(
            "SELECT COUNT(*) AS n FROM messages",
          )
          .get() as { n: number }
      ).n;
    } finally {
      db.close();
    }
  }

  async function expectSendError(
    tools: BridgeTools,
    args: Record<string, unknown>,
    marker: string,
  ): Promise<void> {
    const result = await tools.call(
      "bridge_send",
      args,
    );
    const text =
      result.content[0]?.type === "text"
        ? (result.content[0].text ?? "")
        : "";

    assert.equal(result.isError, true);
    assert.ok(
      text.includes(marker),
      `expected ${marker} in: ${text}`,
    );
  }

  test("v10-1: with no policy set, an untagged send still works", async (t) => {
    const { bus, dbPath } = createV10Bus(t);
    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    const result = await claude.call(
      "bridge_send",
      {
        subject: "v10-1",
        body: "既定では通る",
      },
    );

    assert.notEqual(result.isError, true);
    assert.equal(countMessages(dbPath), 1);
  });

  test("v10-2: with the policy on, an unaddressed send is refused and stores nothing", async (t) => {
    const { bus, dbPath } = createV10Bus(t);
    bus.setRolePolicy("require_tag", "claude,codex");

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await expectSendError(
      claude,
      {
        subject: "v10-2",
        body: "宛先が無い",
      },
      "tag_required",
    );

    assert.equal(countMessages(dbPath), 0);
  });

  test("v10-3: broadcast: true sends role-wide and an undeclared session can claim it", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy("require_tag", "claude,codex");

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    const sent = await claude.call(
      "bridge_send",
      {
        subject: "v10-3",
        body: "role 宛でよい便",
        broadcast: true,
      },
    );

    assert.notEqual(sent.isError, true);

    const fetched = v9Json(
      await codex.call("bridge_fetch", {}),
    );

    assert.equal(fetched.messages.length, 1);
    assert.equal(
      fetched.messages[0]?.to_tag,
      null,
    );
  });

  test("v10-4: to_tag and broadcast together are refused", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: "apps-hub" },
    );

    await expectSendError(
      claude,
      {
        subject: "v10-4",
        body: "宛先が二重",
        to_tag: "winsmux-lane",
        broadcast: true,
      },
      "conflicting_destination",
    );

    assert.equal(countMessages(dbPath), 0);
  });

  test("v10-5: a policy value that does not parse refuses the send", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    /*
     * Written straight into meta: setRequireTagPolicy refuses this
     * value, so the state can only arise from a hand edit or a older
     * writer. That is exactly the case the read has to fail closed on.
     */
    const raw = new Database(dbPath);

    try {
      raw
        .prepare(
          "INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        )
        .run("require_tag", "claude,nope");
    } finally {
      raw.close();
    }

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: "apps-hub" },
    );

    await expectSendError(
      claude,
      {
        subject: "v10-5",
        body: "設定が壊れている",
        to_tag: "winsmux-lane",
      },
      "policy_invalid",
    );

    assert.equal(countMessages(dbPath), 0);
  });

  test("v10-6: a tagged bounce needs the sender to have declared, and goes through once it has", async (t) => {
    const { bus, dbPath } = createV10Bus(t);
    bus.setRolePolicy("require_tag", "claude,codex");

    const undeclared = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await expectSendError(
      undeclared,
      {
        subject: "v10-6 undeclared",
        body: "bounce の宛先が作れない",
        to_tag: "winsmux-lane",
      },
      "sender_tag_required",
    );

    assert.equal(countMessages(dbPath), 0);

    const declared = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await declared.call("bridge_hello", {
      tag: "apps-hub",
    });

    const sent = await declared.call(
      "bridge_send",
      {
        subject: "v10-6 declared",
        body: "bounce の宛先がある",
        to_tag: "winsmux-lane",
      },
    );

    assert.notEqual(sent.isError, true);
    assert.equal(countMessages(dbPath), 1);
  });

  test("v10-6-A: the destination role alone requiring tags is enough to need a declared sender", async (t) => {
    const { bus, dbPath } = createV10Bus(t);
    bus.setRolePolicy("require_tag", "codex");

    const undeclared = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await expectSendError(
      undeclared,
      {
        subject: "v10-6-A",
        body: "宛先側だけ有効",
        to_tag: "winsmux-lane",
      },
      "sender_tag_required",
    );

    assert.equal(countMessages(dbPath), 0);

    const declared = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await declared.call("bridge_hello", {
      tag: "apps-hub",
    });

    const sent = await declared.call(
      "bridge_send",
      {
        subject: "v10-6-A declared",
        body: "宣言すれば通る",
        to_tag: "winsmux-lane",
      },
    );

    assert.notEqual(sent.isError, true);
    assert.equal(countMessages(dbPath), 1);
  });

  test("v10-7: broadcast is not part of the envelope, so a resend stays idempotent", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    const messageId =
      "88888888-8888-4888-8888-88888888a107";

    const first = await claude.call(
      "bridge_send",
      {
        subject: "v10-7",
        body: "同じ封筒",
        message_id: messageId,
      },
    );
    const second = await claude.call(
      "bridge_send",
      {
        subject: "v10-7",
        body: "同じ封筒",
        message_id: messageId,
        broadcast: true,
      },
    );

    assert.notEqual(first.isError, true);
    assert.notEqual(second.isError, true);

    const secondText =
      second.content[0]?.type === "text"
        ? (second.content[0].text ?? "")
        : "";

    assert.ok(
      secondText.includes("idempotent"),
      secondText,
    );
    assert.equal(countMessages(dbPath), 1);
  });

  test("v10-8: the policy is read per send, not cached when the bus opens", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    const before = await claude.call(
      "bridge_send",
      {
        subject: "v10-8 before",
        body: "ポリシー前",
      },
    );

    assert.notEqual(before.isError, true);

    bus.setRolePolicy("require_tag", "codex");

    await expectSendError(
      claude,
      {
        subject: "v10-8 after",
        body: "ポリシー後",
      },
      "tag_required",
    );

    assert.equal(countMessages(dbPath), 1);
  });

  test("v10-9: an exact retry still succeeds after the policy is enabled", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    const messageId =
      "99999999-9999-4999-8999-99999999a109";

    const first = await claude.call(
      "bridge_send",
      {
        subject: "v10-9",
        body: "応答が失われた便",
        message_id: messageId,
      },
    );

    assert.notEqual(first.isError, true);

    bus.setRolePolicy("require_tag", "claude,codex");

    /*
     * The turn-head rule tells a sender whose response went missing to
     * retry with the same id. That retry stores nothing, so refusing it
     * because the policy changed in between would push the sender
     * toward a new id, which is the duplicate the idempotency key
     * exists to prevent.
     */
    const retry = await claude.call(
      "bridge_send",
      {
        subject: "v10-9",
        body: "応答が失われた便",
        message_id: messageId,
      },
    );

    const retryText =
      retry.content[0]?.type === "text"
        ? (retry.content[0].text ?? "")
        : "";

    assert.notEqual(retry.isError, true);
    assert.ok(
      retryText.includes("idempotent"),
      retryText,
    );
    assert.equal(countMessages(dbPath), 1);

    await expectSendError(
      claude,
      {
        subject: "v10-9 new",
        body: "新しい便は拒否される",
      },
      "tag_required",
    );
  });

  test("v11-1: only the process a message was presented to can acknowledge it", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const receiver = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );
    const bystander = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v11-1",
      body: "受領者だけが終端できる",
    });

    const fetched = v9Json(
      await receiver.call("bridge_fetch", {}),
    );
    const message = fetched.messages[0];

    assert.ok(message);

    /*
     * The bystander reads the attempt id the way anything on this
     * machine can: out of bridge_status, which is deliberately not
     * filtered by role or tag so a sender can follow its own mail.
     */
    const statusText =
      (
        await bystander.call(
          "bridge_status",
          {
            message_id: message.message_id,
          },
        )
      ).content[0]?.text ?? "";
    const seen = JSON.parse(statusText) as {
      message: { attempt_id: string };
    };

    assert.equal(
      seen.message.attempt_id,
      message.attempt_id,
    );

    const stolen = await bystander.call(
      "bridge_ack",
      {
        message_id: message.message_id,
        attempt_id: seen.message.attempt_id,
      },
    );

    assert.equal(stolen.isError, true);
    assert.equal(
      bus.status(message.message_id).message
        .status,
      "presented",
    );

    const owned = await receiver.call(
      "bridge_ack",
      {
        message_id: message.message_id,
        attempt_id: message.attempt_id,
      },
    );

    assert.notEqual(owned.isError, true);
    assert.equal(
      bus.status(message.message_id).message
        .status,
      "acked",
    );
  });

  test("v11-2: the attempt id a failed ack hands back does not help either", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const receiver = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );
    const bystander = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v11-2",
      body: "失敗応答から読み取っても通らない",
    });

    const fetched = v9Json(
      await receiver.call("bridge_fetch", {}),
    );
    const message = fetched.messages[0];

    assert.ok(message);

    /*
     * A wrong guess is answered with the current state, so the failure
     * itself is a third way to learn the attempt id. That is why the
     * fix is the consumer condition and not hiding the value.
     */
    const guessed = await bystander.call(
      "bridge_ack",
      {
        message_id: message.message_id,
        attempt_id:
          "12345678-1234-4234-8234-123456789abc",
      },
    );

    const guessedText =
      guessed.content[0]?.text ?? "";

    assert.equal(guessed.isError, true);
    assert.ok(
      message.attempt_id !== null &&
        guessedText.includes(
          message.attempt_id,
        ),
      guessedText,
    );

    const retried = await bystander.call(
      "bridge_ack",
      {
        message_id: message.message_id,
        attempt_id: message.attempt_id,
      },
    );

    assert.equal(retried.isError, true);
    assert.equal(
      bus.status(message.message_id).message
        .status,
      "presented",
    );
  });

  test("v12-7: the hook notice changes when strict addressing is on", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v12-7",
      body: "宛先なしの便",
      now: T0,
    });

    const before = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v12-7" },
    );

    assert.equal(
      before.stderr.includes(
        "strict_addressing",
      ),
      false,
      before.stderr,
    );
    assert.ok(
      before.stdout.includes(
        "bridge_fetch(peek=true, limit=10)",
      ),
      before.stdout,
    );

    bus.setRolePolicy(
      "strict_addressing",
      "claude",
    );
    bus.close();

    const after = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v12-7" },
    );

    /*
     * The counts cannot tell an undeclared session apart from the
     * addressee, so the notice has to carry that difference. Without
     * this the numbers say one thing and a fetch returns nothing,
     * which is issue #2 with a new cause.
     */
    assert.ok(
      after.stdout.includes(
        "strict_addressing",
      ),
      after.stdout,
    );
    /*
     * This assertion used to require the opposite, which is how the
     * strict branch shipped with no instruction to read anything. The
     * caveat is what differs between the branches; the peek call is
     * shared and stays outside them.
     */
    assert.ok(
      after.stdout.includes(
        "bridge_fetch(peek=true, limit=10)",
      ),
      after.stdout,
    );
  });

  test("v12-1: with no strict policy, an undeclared session still takes untagged mail", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v12-1",
      body: "既定では未宣言でも取れる",
    });

    const fetched = v9Json(
      await codex.call("bridge_fetch", {}),
    );

    assert.equal(fetched.messages.length, 1);
  });

  test("v12-2: strict addressing hides untagged mail from a session that declared nothing", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy(
      "strict_addressing",
      "codex",
    );

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const undeclared = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );
    const declared = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v12-2",
      body: "宣言しなければ見えない",
    });

    const hidden = v9Json(
      await undeclared.call(
        "bridge_fetch",
        {},
      ),
    );

    assert.equal(hidden.messages.length, 0);
    assert.equal(hidden.declared_tag, null);

    /*
     * The zero above only means something next to this: a predicate
     * that hid everything would pass the first half on its own.
     */
    await declared.call("bridge_hello", {
      tag: "winsmux-lane",
    });

    const visible = v9Json(
      await declared.call(
        "bridge_fetch",
        {},
      ),
    );

    assert.equal(visible.messages.length, 1);
    assert.equal(
      visible.messages[0]?.subject,
      "v12-2",
    );
  });

  test("v12-3: strict addressing leaves tagged routing alone", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy(
      "strict_addressing",
      "codex",
    );

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: "apps-hub" },
    );
    const other = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );
    const owner = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v12-3",
      body: "レーン宛",
      to_tag: "winsmux-lane",
    });

    await other.call("bridge_hello", {
      tag: "x-jimaku-lane",
    });

    const wrongTag = v9Json(
      await other.call("bridge_fetch", {}),
    );

    assert.equal(
      wrongTag.messages.length,
      0,
    );

    await owner.call("bridge_hello", {
      tag: "winsmux-lane",
    });

    const rightTag = v9Json(
      await owner.call("bridge_fetch", {}),
    );

    assert.equal(
      rightTag.messages.length,
      1,
    );
  });

  test("v12-4: the counts follow the same predicate as the fetch", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy(
      "strict_addressing",
      "codex",
    );

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const undeclared = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    for (const n of [1, 2, 3, 4]) {
      await claude.call("bridge_send", {
        subject: `v12-4 ${n}`,
        body: `本文 ${n}`,
      });
    }

    const peeked = v9Json(
      await undeclared.call("bridge_fetch", {
        peek: true,
        limit: 1,
      }),
    );

    assert.equal(peeked.messages.length, 0);
    assert.equal(peeked.has_more, false);
    assert.equal(
      peeked.unacked_total,
      0,
    );

    /*
     * Zero on its own would also come from counts that always return
     * zero. The declared side has to see the same four, and the
     * non-peek path counts through a different connection.
     */
    const declared = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await declared.call("bridge_hello", {
      tag: "winsmux-lane",
    });

    const declaredPeek = v9Json(
      await declared.call("bridge_fetch", {
        peek: true,
        limit: 1,
      }),
    );

    assert.equal(
      declaredPeek.messages.length,
      1,
    );
    assert.equal(
      declaredPeek.has_more,
      true,
    );
    assert.equal(
      declaredPeek.unacked_total,
      4,
    );

    const claimed = v9Json(
      await declared.call("bridge_fetch", {
        limit: 1,
      }),
    );

    assert.equal(
      claimed.messages.length,
      1,
    );
    assert.equal(claimed.has_more, true);
  });

  test("v12-5: a strict policy that does not parse refuses the fetch", async (t) => {
    const { bus, dbPath } = createV10Bus(t);

    const raw = new Database(dbPath);

    try {
      raw
        .prepare(
          "INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
        )
        .run("strict_addressing", "codex,nope");
    } finally {
      raw.close();
    }

    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    const result = await codex.call(
      "bridge_fetch",
      {},
    );
    const text =
      result.content[0]?.type === "text"
        ? (result.content[0].text ?? "")
        : "";

    assert.equal(result.isError, true);
    assert.ok(
      text.includes("policy_invalid"),
      text,
    );
    assert.ok(
      text.includes("strict_addressing"),
      text,
    );
  });

  test("v12-6: enabling it for one role leaves the other role alone", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy(
      "strict_addressing",
      "codex",
    );

    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );
    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await codex.call("bridge_send", {
      subject: "v12-6",
      body: "claude 役は従来どおり",
    });

    const fetched = v9Json(
      await claude.call("bridge_fetch", {}),
    );

    assert.equal(fetched.messages.length, 1);
  });

  function makeDocRepo(
    t: TestContext,
    files: Record<string, string>,
  ): string {
    const root = mkdtempSync(
      join(
        tmpdir(),
        "agent-bridge-docs-",
      ),
    );

    t.after(() => {
      rmSync(root, {
        recursive: true,
        force: true,
      });
    });

    for (const [name, body] of Object.entries(
      files,
    )) {
      const full = join(root, name);
      mkdirSync(dirname(full), {
        recursive: true,
      });
      writeFileSync(full, body, "utf8");
    }

    const git = (
      ...args: readonly string[]
    ): void => {
      execFileSync("git", args, {
        cwd: root,
        stdio: "ignore",
      });
    };

    git("init");
    git("config", "user.email", "t@example.com");
    git("config", "user.name", "t");
    git("add", ".");
    git("commit", "-m", "docs");

    return root;
  }

  test("v13-1: a published document pointing at a file the tree lacks is reported", (t) => {
    const kept = makeDocRepo(t, {
      "README.md":
        "see [the guide](docs/deploy.md)\n",
      "docs/deploy.md": "guide\n",
    });

    assert.deepEqual(
      checkReferences(kept),
      [],
    );

    const dropped = makeDocRepo(t, {
      "README.md":
        "see [the guide](docs/e2e-checklist.md)\n",
      "docs/deploy.md": "guide\n",
    });
    const findings =
      checkReferences(dropped);

    assert.equal(findings.length, 1);
    assert.ok(
      findings[0]?.detail.includes(
        "docs/e2e-checklist.md",
      ),
      findings[0]?.detail,
    );
  });

  test("v13-2: only the documents a reader operates from are held to it", (t) => {
    const root = makeDocRepo(t, {
      "README.md": "clean\n",
      "docs/design-v3.md":
        "the notifier lived in `src/notifier.ts`\n",
      "docs/reviews/old.md":
        "see [it](src/notifier.ts)\n",
    });

    /*
     * Design notes and review transcripts describe states that are gone
     * on purpose. Reporting them would train the reader to ignore this.
     */
    assert.deepEqual(
      checkReferences(root),
      [],
    );
  });

  test("v13-5: a canonical block is found when the source file uses CRLF", (t) => {
    /*
     * Windows is the documented platform and a normal checkout there
     * has CRLF. A regex anchored on bare newlines finds no block at
     * all, and the run then reports zero problems and zero skips,
     * which reads exactly like a genuine pass.
     */
    const root = makeDocRepo(t, {
      "README.md": "x\n",
      "docs/deploy.md":
        "<!-- canonical: rule -->\r\n```markdown\r\n## rule\r\n\r\n- first\r\n```\r\n",
    });

    const target = join(root, "t.md");

    writeFileSync(
      target,
      "# local\n\n## rule\n\n- first\n",
      "utf8",
    );

    assert.deepEqual(
      checkTranscripts(
        root,
        new Map([["rule", target]]),
      ),
      [],
    );

    writeFileSync(
      target,
      "# local\n\nnothing like it\n",
      "utf8",
    );

    const findings = checkTranscripts(
      root,
      new Map([["rule", target]]),
    );

    assert.equal(findings.length, 1);
    assert.equal(
      isSkip(findings[0]!),
      false,
    );
  });

  test("v13-6: a link to a root-level file the tree lacks is reported", (t) => {
    const missing = makeDocRepo(t, {
      "README.md":
        "[japanese](README.ja.md)\n",
      "docs/deploy.md": "x\n",
    });
    const findings =
      checkReferences(missing);

    assert.equal(findings.length, 1);
    assert.ok(
      findings[0]?.detail.includes(
        "README.ja.md",
      ),
      findings[0]?.detail,
    );

    const present = makeDocRepo(t, {
      "README.md":
        "[japanese](README.ja.md)\n",
      "README.ja.md": "x\n",
      "docs/deploy.md": "x\n",
    });

    assert.deepEqual(
      checkReferences(present),
      [],
    );
  });

  test("v13-7: bare inline code is not treated as a path", (t) => {
    /*
     * Links say navigate here. Inline code is a guess, so it keeps the
     * directory-separator requirement; `settings.json` appears in the
     * guide as a file the reader owns, not one this tree ships.
     */
    const root = makeDocRepo(t, {
      "README.md":
        "call `bridge_fetch`, merge into `settings.json`\n",
      "docs/deploy.md": "x\n",
    });

    assert.deepEqual(
      checkReferences(root),
      [],
    );
  });

  test("v13-3: a transcription that drifted from its canonical block is reported", (t) => {
    const root = makeDocRepo(t, {
      "README.md": "x\n",
      "docs/deploy.md":
        "<!-- canonical: rule -->\n```markdown\n## rule\n\n- first\n- second\n```\n",
    });

    const target = join(
      root,
      "transcribed.md",
    );

    writeFileSync(
      target,
      "# local\n\n## rule\n\n- first\n- second\n\nlocal extras below\n",
      "utf8",
    );

    assert.deepEqual(
      checkTranscripts(
        root,
        new Map([["rule", target]]),
      ),
      [],
    );

    writeFileSync(
      target,
      "# local\n\n## rule\n\n- first\n- changed\n",
      "utf8",
    );

    const drifted = checkTranscripts(
      root,
      new Map([["rule", target]]),
    );

    assert.equal(drifted.length, 1);
    assert.equal(
      isSkip(drifted[0]!),
      false,
    );
  });

  test("v13-4: a target that is not on this machine is skipped, not failed", (t) => {
    const root = makeDocRepo(t, {
      "README.md": "x\n",
      "docs/deploy.md":
        "<!-- canonical: rule -->\n```markdown\n## rule\n```\n",
    });

    const absent = checkTranscripts(
      root,
      new Map([
        [
          "rule",
          join(root, "not-here.md"),
        ],
      ]),
    );

    assert.equal(absent.length, 1);
    assert.equal(
      isSkip(absent[0]!),
      true,
    );

    const unnamed = checkTranscripts(
      root,
      new Map(),
    );

    assert.equal(unnamed.length, 1);
    assert.equal(
      isSkip(unnamed[0]!),
      true,
    );
  });

  test("v15-1: the exported claim path obeys strict addressing too", async (t) => {
    const { bus } = createV10Bus(t);
    bus.setRolePolicy(
      "strict_addressing",
      "codex",
    );

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v15-1",
      body: "未宣言では取れない",
    });

    /*
     * A default of false on the exported method would leave the
     * invariant with whoever calls it rather than with the transition.
     */
    assert.equal(
      bus.claim(
        "codex",
        createConsumerId("codex"),
        3,
        Date.now(),
        null,
      ).length,
      0,
    );

    assert.equal(
      bus.claim(
        "codex",
        createConsumerId("codex"),
        3,
        Date.now(),
        "winsmux-lane",
      ).length,
      1,
    );
  });

  test("v15-2: an idempotent retry does not report the policy as absent", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );

    const messageId =
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaa151";

    await claude.call("bridge_send", {
      subject: "v15-2",
      body: "先に送っておく",
      message_id: messageId,
    });

    bus.setRolePolicy(
      "require_tag",
      "claude,codex",
    );

    const retry = await claude.call(
      "bridge_send",
      {
        subject: "v15-2",
        body: "先に送っておく",
        message_id: messageId,
      },
    );

    const text =
      retry.content[0]?.type === "text"
        ? (retry.content[0].text ?? "")
        : "";

    assert.notEqual(retry.isError, true);
    assert.ok(
      text.includes("idempotent"),
      text,
    );

    /*
     * The retry returns before the policy is consulted, so it must not
     * describe the policy at all. Saying it is unset while it is on
     * sends the reader off with the wrong routing rule.
     */
    assert.equal(
      text.includes("require_tag"),
      false,
      text,
    );
  });

  test("v15-3: the hook tells a session to peek before it fetches", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v15-3",
      body: "宛先の判断より先に本文が来ないこと",
      now: T0,
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v15-3" },
    );

    /*
     * `bridge_fetch` with no arguments defaults to peek false, so an
     * instruction to call it first claims up to three messages and
     * hands over their bodies before the session can tell whose they
     * are. The tools can route before reading; the instruction has to
     * ask for it.
     */
    assert.ok(
      notice.stdout.includes(
        "bridge_fetch(peek=true, limit=10)",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "bridge_fetch(message_id=",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes("next_cursor"),
      notice.stdout,
    );
    assert.equal(
      notice.stdout.includes(
        "ならbridge_fetchを呼んでください",
      ),
      false,
      notice.stdout,
    );
  });

  test("v16-1: peek pages forward instead of returning the same rows", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: null },
    );
    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    for (const n of [1, 2, 3, 4, 5]) {
      await claude.call("bridge_send", {
        subject: `v16-1 ${n}`,
        body: `本文 ${n}`,
      });
    }

    /*
     * The old rule said to repeat the call while has_more. Peek changes
     * no state and orders by id, so a session that leaves the first
     * page for someone else never reaches what is behind it.
     */
    const first = v9Json(
      await codex.call("bridge_fetch", {
        peek: true,
        limit: 3,
      }),
    );

    assert.equal(first.messages.length, 3);
    assert.equal(first.has_more, true);
    assert.ok(
      first.next_cursor !== null &&
        first.next_cursor !== undefined,
      JSON.stringify(first),
    );

    const second = v9Json(
      await codex.call("bridge_fetch", {
        peek: true,
        limit: 3,
        cursor: first.next_cursor,
      }),
    );

    assert.equal(second.messages.length, 2);
    assert.equal(second.has_more, false);
    assert.equal(second.next_cursor, null);

    const seen = [
      ...first.messages,
      ...second.messages,
    ].map((m) => m.subject);

    assert.deepEqual(seen, [
      "v16-1 1",
      "v16-1 2",
      "v16-1 3",
      "v16-1 4",
      "v16-1 5",
    ]);
  });

  test("v16-2: a cursor does not widen what a session may see", async (t) => {
    const { bus } = createV10Bus(t);

    const claude = new BridgeTools(
      bus,
      "claude",
      createConsumerId("claude"),
      { tag: "apps-hub" },
    );
    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    await claude.call("bridge_send", {
      subject: "v16-2 first",
      body: "誰でも見える",
    });
    await claude.call("bridge_send", {
      subject: "v16-2 tagged",
      body: "レーン宛",
      to_tag: "winsmux-lane",
    });
    await claude.call("bridge_send", {
      subject: "v16-2 second",
      body: "誰でも見える",
    });

    const paged = v9Json(
      await codex.call("bridge_fetch", {
        peek: true,
        limit: 1,
      }),
    );

    assert.equal(paged.messages.length, 1);

    /*
     * Paging walks the rows this session may see. Stepping past the
     * first must not step into rows the predicate excluded.
     */
    const next = v9Json(
      await codex.call("bridge_fetch", {
        peek: true,
        limit: 10,
        cursor: paged.next_cursor,
      }),
    );

    assert.equal(next.messages.length, 1);
    assert.equal(
      next.messages[0]?.subject,
      "v16-2 second",
    );
    assert.equal(next.has_more, false);
  });

  test("v16-3: a non-peek fetch rejects a cursor", async (t) => {
    const { bus } = createV10Bus(t);

    const codex = new BridgeTools(
      bus,
      "codex",
      createConsumerId("codex"),
      { tag: null },
    );

    const result = await codex.call(
      "bridge_fetch",
      { cursor: 1 },
    );
    const text =
      result.content[0]?.type === "text"
        ? (result.content[0].text ?? "")
        : "";

    assert.equal(result.isError, true);
    assert.ok(
      text.includes("cursor"),
      text,
    );
  });

  test("v16-4: the notice says the window ends and the cursor does not carry", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v16-4",
      body: "窓の外は次のターンでも読み直せない",
      now: T0,
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v16-4" },
    );

    /*
     * Paging within a turn was the fix. Across turns the reader starts at
     * the head again, so an exhausted window is a backlog a person has to
     * clear, not something the next turn picks up.
     */
    assert.ok(
      notice.stdout.includes(
        "次のターンへ持ち越さず",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "待っても解消しません",
      ),
      notice.stdout,
    );
  });

  test("v17-1: the sweep line counts untagged mail that nothing terminates", (t) => {
    const { dbPath } = makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v17-1 untagged",
        body: "誰も取らなければ残り続ける",
        now: T0,
      });
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v17-1 tagged",
        body: "宛先があるので期限で終端できる",
        toTag: "v17-lane",
        onTimeout: "bounce",
        now: T0 + 1_000,
      });

      const backlog = bus.backlog("claude");

      assert.equal(backlog.untagged, 1);
      assert.equal(
        backlog.oldestSentAt,
        new Date(T0).toISOString(),
      );
      assert.equal(
        formatBacklog(
          backlog,
          T0 + 7_200_000,
        ),
        "untagged:1,oldest:2h",
      );
      assert.equal(
        formatBacklog(
          {
            untagged: 0,
            oldestSentAt: null,
          },
          T0,
        ),
        "untagged:0,oldest:-",
      );
    } finally {
      bus.close();
    }
  });

  test("v18-1: a strict session is told to peek, not only to declare", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v18-1",
      body: "宣言しただけでは何も読まない",
      now: T0,
    });
    bus.setRolePolicy(
      "strict_addressing",
      "claude",
    );
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v18-1" },
    );

    /*
     * The rest of the notice describes what to do with a peek result,
     * so a branch that never asks for one leaves the addressee with
     * instructions about something it was never told to obtain.
     */
    assert.ok(
      notice.stdout.includes(
        "strict_addressing",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "bridge_fetch(peek=true, limit=10)",
      ),
      notice.stdout,
    );
  });

  test("v18-2: when only expired rows are pending, the notice says peek cannot reach them", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v18-2",
      body: "lease が切れたまま残る",
      now: T0,
    });

    const claimed = bus.claim(
      "claude",
      createConsumerId("claude"),
      1,
      T0,
    );
    assert.equal(claimed.length, 1);
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v18-2" },
    );

    /*
     * The row is counted as fetchable and peek selects only stored, so
     * without this the notice sends the session around a loop it cannot
     * leave: peek returns nothing, and the rule forbids the bare fetch
     * that would have run recovery.
     */
    assert.ok(
      notice.stdout.includes(
        "期限切れのclaimed・presented・tag",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes("bridge-sweep"),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "peekが実際に0件を返したときだけ",
      ),
      notice.stdout,
    );
  });

  test("v18-3: the same notice keeps its ordinary shape while stored mail exists", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v18-3",
      body: "取れる便があるとき",
      now: T0,
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v18-3" },
    );

    assert.equal(
      notice.stdout.includes("bridge-sweep"),
      false,
      notice.stdout,
    );
  });

  test("v19-1: the follow-up peek keeps the limit instead of falling back to three", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v19-1",
      body: "頁送りの取り分",
      now: T0,
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v19-1" },
    );

    /*
     * limit defaults to DEFAULT_FETCH_LIMIT, so a cursor call written
     * without it reads three rows. Five calls then reach 22 rows while
     * the rule around them claims 50.
     */
    assert.ok(
      notice.stdout.includes(
        "bridge_fetch(peek=true, limit=10, cursor=<その値>)",
      ),
      notice.stdout,
    );
    assert.equal(
      notice.stdout.includes(
        "bridge_fetch(peek=true, cursor=",
      ),
      false,
      notice.stdout,
    );
  });

  test("v19-2: the rule and the notice quote the same ceiling", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v19-2",
      body: "文書と通知が別々に書かれている",
      now: T0,
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v19-2" },
    );
    const rule = readFileSync(
      join(PROJECT_ROOT, "docs", "deploy.md"),
      "utf8",
    ).replace(/\r\n/g, "\n");

    for (const call of [
      "bridge_fetch(peek=true, limit=10)",
      "bridge_fetch(peek=true, limit=10, cursor=<その値>)",
    ]) {
      assert.ok(rule.includes(call), call);
      assert.ok(notice.stdout.includes(call), call);
    }
  });

  test("v20-1: recovery_owed separates an expired row from a live delivery", (t) => {
    const { dbPath } = makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v20-1 expired",
        body: "lease が切れている",
        now: T0,
      });
      bus.claim(
        "claude",
        createConsumerId("claude"),
        1,
        T0,
      );

      const live = T0 + 10 * TAG_TTL_MS;
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v20-1 live",
        body: "別セッションが持っている",
        now: live,
      });
      bus.claim(
        "claude",
        createConsumerId("claude"),
        1,
        live,
      );

      const peeked = bus.fetch(
        "claude",
        createConsumerId("claude"),
        { peek: true, now: live },
      );

      /*
       * Both rows are claimed and neither is stored, so unacked_total
       * counts two and the page is empty. Only one of them is waiting
       * for the sweep, and the rule needs to tell them apart before it
       * reports a backlog.
       */
      assert.deepEqual(peeked.messages, []);
      assert.equal(peeked.unacked_total, 2);
      assert.equal(peeked.recovery_owed, 1);
    } finally {
      bus.close();
    }
  });

  test("v20-2: an expired row does not send a tagged addressee away", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v20-2 expired",
      body: "回収待ち",
      now: T0,
    });
    bus.claim(
      "claude",
      createConsumerId("claude"),
      1,
      T0,
    );

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v20-2 live",
      body: "宣言すれば見える便",
      toTag: "v20-lane",
      now: Date.now(),
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v20-2" },
    );

    /*
     * stored is zero here because the live row carries a tag, so a
     * condition written on stored would have told the addressee of
     * that row to end its turn, and to keep doing so for as long as
     * the expired row sat there.
     */
    assert.ok(
      notice.stdout.includes(
        "peekが便を返したなら、それは通常どおり処理します",
      ),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "bridge_fetch(peek=true, limit=10)",
      ),
      notice.stdout,
    );
  });

  test("v20-3: stored mail alongside an expired row still reports the expired one", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v20-3 expired",
      body: "回収待ち",
      now: T0,
    });
    bus.claim(
      "claude",
      createConsumerId("claude"),
      1,
      T0,
    );

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v20-3 stored",
      body: "普通に取れる便",
      now: Date.now(),
    });
    bus.close();

    const notice = await runHookProcess(
      "stop",
      userProfile,
      { session_id: "v20-3" },
    );

    /*
     * A condition written as stored === 0 goes quiet here, because one
     * ordinary message hides the expired row behind it. The sentence is
     * about the expired rows, so it keys on those.
     */
    assert.ok(
      notice.stdout.includes("bridge-sweep"),
      notice.stdout,
    );
    assert.ok(
      notice.stdout.includes(
        "取得可能のうち1件は期限切れ",
      ),
      notice.stdout,
    );
  });

  test("v21-1: an addressee does not see its own mail once the tag has expired", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      const sent = bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v21-1",
        body: "宛先は自分だが期限が切れている",
        toTag: "v21-lane",
        now: T0,
      });

      const after = T0 + TAG_TTL_MS + 1;
      const consumer =
        createConsumerId("claude");

      /*
       * The row is still stored and still carries the tag, so the
       * predicate that hides other lanes does not hide this one from
       * its own addressee. It saw its message, fetched it, and the
       * fetch recovered before it claimed, so the reply was empty and
       * the row was bounced. Peek shows what a fetch can deliver.
       */
      const addressee = bus.fetch(
        "claude",
        consumer,
        {
          peek: true,
          now: after,
          tag: "v21-lane",
        },
      );

      assert.deepEqual(
        addressee.messages,
        [],
      );
      assert.equal(
        addressee.recovery_owed,
        1,
      );

      const elsewhere = bus.fetch(
        "claude",
        consumer,
        {
          peek: true,
          now: after,
          tag: "another-lane",
        },
      );

      assert.deepEqual(
        elsewhere.messages,
        [],
      );
      assert.equal(
        elsewhere.recovery_owed,
        1,
      );

      /*
       * Named directly it stays hidden too, so a session cannot reach
       * around the page to a row the next fetch would bounce.
       */
      const named = bus.fetch(
        "claude",
        consumer,
        {
          peek: true,
          now: after,
          tag: "v21-lane",
          messageId: sent.messageId,
        },
      );

      assert.deepEqual(named.messages, []);
    } finally {
      bus.close();
    }
  });

  test("v21-2: a live tag is still visible to its addressee", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v21-2",
        body: "期限内なので見える",
        toTag: "v21-lane",
        now: T0,
      });

      const peeked = bus.fetch(
        "claude",
        createConsumerId("claude"),
        {
          peek: true,
          now: T0 + 60_000,
          tag: "v21-lane",
        },
      );

      assert.deepEqual(
        peeked.messages.map(
          (message) => message.subject,
        ),
        ["v21-2"],
      );
      assert.equal(
        peeked.recovery_owed,
        0,
      );
    } finally {
      bus.close();
    }
  });

  test("v22-1: a loss is reported even after an agent has acknowledged the notice", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      const sent = bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v22-1 の設計文書",
        body: "宛先が来ないまま期限切れ",
        toTag: "gone-lane",
        fromTag: "sender-lane",
        now: T0,
      });

      const after = T0 + TAG_TTL_MS + 1;
      bus.recover("claude", after);

      /*
       * The bounce goes back to the sender as an ordinary message, and an
       * agent there takes it. That is the state the first version of this
       * report treated as "somebody knows", which returned nothing for all
       * six real losses while the person still had to count rows. Acking
       * it here is what makes the condition testable at all.
       */
      const consumer = createConsumerId("codex");
      const notices = bus.claim(
        "codex",
        consumer,
        3,
        after,
        "sender-lane",
      );
      assert.equal(notices.length, 1);
      bus.markPresented(
        "codex",
        consumer,
        [
          {
            messageId: notices[0]!.message_id,
            attemptId: notices[0]!.attempt_id,
          },
        ],
        after,
      );
      bus.ack(
        "codex",
        notices[0]!.message_id,
        notices[0]!.attempt_id,
        after,
        consumer,
      );

      const first = bus.undelivered(
        "claude",
        null,
        5,
      );

      assert.equal(first.lost.length, 1);
      assert.equal(
        first.lost[0]?.subject,
        "v22-1 の設計文書",
      );
      assert.equal(
        first.lost[0]?.toTag,
        "gone-lane",
      );
      assert.equal(first.lostSince, 1);
      assert.equal(first.lostTotal, 1);

      /*
       * The window is exclusive at its own edge. In the sweep the cursor
       * is written as the timestamp of a reported row, so an inclusive
       * comparison would repeat that row on every run forever.
       */
      const atSame = bus.undelivered(
        "claude",
        first.lost[0]!.seq,
        5,
      );
      assert.deepEqual(atSame.lost, []);
      assert.equal(atSame.lostTotal, 1);
    } finally {
      bus.close();
    }
  });

  test("v22-2: the cursor only moves forward and the completion stamp always moves", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      assert.equal(
        bus.readSweepMark("claude"),
        null,
      );
      assert.equal(
        bus.readSweepCompletedAt(),
        null,
      );

      const early = 7;
      const later = 12;

      bus.writeSweepMark(
        "claude",
        later,
        T0 + 60_000,
      );
      assert.equal(
        bus.readSweepMark("claude"),
        later,
      );

      /* The other role keeps its own place. */
      assert.equal(
        bus.readSweepMark("codex"),
        null,
      );

      /*
       * Two sweeps at once both read the older cursor. The slower one
       * finishing last must not drag it back over ground the other has
       * already reported, or those losses are stepped over in silence.
       */
      bus.writeSweepMark(
        "claude",
        early,
        T0 + 120_000,
      );
      assert.equal(
        bus.readSweepMark("claude"),
        later,
      );

      /* Liveness is a separate question and always records. */
      assert.equal(
        bus.readSweepCompletedAt(),
        new Date(T0 + 120_000).toISOString(),
      );
    } finally {
      bus.close();
    }
  });

  test("v22-3: the page is cut in the query, not after loading", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      for (let index = 0; index < 7; index += 1) {
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: `v22-3 loss ${index}`,
          body: "x",
          toTag: `gone-${index}`,
          now: T0 + index * 1_000,
        });
      }

      bus.recover(
        "claude",
        T0 + TAG_TTL_MS + 10_000,
      );

      const report = bus.undelivered(
        "claude",
        null,
        5,
      );

      /*
       * Nothing prunes messages or events, so a slice taken after loading
       * grows with the whole history of the deployment.
       */
      assert.equal(report.lost.length, 5);
      assert.equal(report.lostSince, 7);
      assert.equal(report.lostTotal, 7);

      /* Oldest first, so the cursor can page forward through the rest. */
      assert.deepEqual(
        report.lost.map(
          (row) => row.subject,
        ),
        [0, 1, 2, 3, 4].map(
          (index) => `v22-3 loss ${index}`,
        ),
      );

      const next = bus.undelivered(
        "claude",
        report.lost[4]!.seq,
        5,
      );
      assert.deepEqual(
        next.lost.map((row) => row.subject),
        ["v22-3 loss 5", "v22-3 loss 6"],
      );
    } finally {
      bus.close();
    }
  });

  test("v22-4: a capped page says how much it left for the next sweep", () => {
    const rows = Array.from(
      { length: 5 },
      (_, index) => ({
        subject: `subject ${index}`,
        toTag: "lane",
        at: new Date(T0).toISOString(),
      }),
    );

    const lines = formatUndelivered(
      "claude",
      {
        lost: rows,
        lostSince: 7,
        lostTotal: 7,
      },
      T0 + 2 * 3_600_000,
    );

    /*
     * A capped list that hides its own remainder reads as a complete one,
     * which is the shape that turns a truncated report into a false
     * all-clear.
     */
    assert.ok(
      lines.some((line) =>
        line.includes(
          "(+2 not listed, carried to the next sweep)",
        ),
      ),
      lines.join("\n"),
    );
    assert.equal(
      lines.filter((line) =>
        line.startsWith("  2h0m ->"),
      ).length,
      5,
      lines.join("\n"),
    );
  });

  test("v22-5: nothing to report prints nothing", () => {
    assert.deepEqual(
      formatUndelivered(
        "codex",
        {
          lost: [],
          lostSince: 0,
          lostTotal: 0,
        },
        T0,
      ),
      [],
    );
  });

  test("v22-6: age carries both units, and a subject cannot split the line", () => {
    assert.equal(
      formatAge(
        new Date(T0).toISOString(),
        T0 + 89 * 60_000,
      ),
      "1h29m",
    );
    assert.equal(
      formatAge(
        new Date(T0).toISOString(),
        T0 + 90 * 60_000,
      ),
      "1h30m",
    );

    /*
     * Planted in the tag as well as the subject. The first version of
     * this escaped the subject and left the tag beside it raw, so a
     * separator in a destination split the line and the half after it
     * read as a log entry nobody wrote.
     */
    const separators = [0x2028, 0x2029].map((code) =>
      String.fromCharCode(code),
    );

    const rendered = formatUndelivered(
      "claude",
      {
        lost: [
          {
            subject: `before${separators[0]}after`,
            toTag: `lane${separators[1]}forged`,
            at: new Date(T0).toISOString(),
            seq: 1,
          },
        ],
        lostSince: 1,
        lostTotal: 1,
      },
      T0 + 60_000,
    );

    for (const line of rendered) {
      for (const separator of separators) {
        assert.equal(
          line.includes(separator),
          false,
          line,
        );
      }
    }
  });

  test("v23-1: a control character in an operator command is reported", (t) => {
    const repoRoot = mkdtempSync(
      join(tmpdir(), "agent-bridge-ctrl-"),
    );
    t.after(() =>
      rmSync(repoRoot, {
        recursive: true,
        force: true,
      }),
    );

    execFileSync("git", ["init", "-q"], {
      cwd: repoRoot,
    });

    /*
     * The exact shape that shipped: a halved backslash escape turned
     * \\b into U+0008, so the registration command named a path that
     * does not exist and every viewer rendered it as if it did.
     */
    const damaged = `node <repo>\\dist${String.fromCharCode(
      8,
    )}ridge-sweep.js`;
    writeFileSync(
      join(repoRoot, "README.md"),
      `# t\n\n\`\`\`powershell\n${damaged}\n\`\`\`\n`,
      "utf8",
    );
    execFileSync(
      "git",
      ["add", "README.md"],
      { cwd: repoRoot },
    );

    const before =
      checkControlCharacters(repoRoot);
    assert.equal(before.length, 1);
    assert.ok(
      before[0]?.detail.includes("U+0008"),
      JSON.stringify(before),
    );
    assert.ok(
      before[0]?.detail.startsWith(
        "README.md:4",
      ),
      JSON.stringify(before),
    );

    writeFileSync(
      join(repoRoot, "README.md"),
      "# t\n\n```powershell\nnode <repo>\\dist\\bridge-sweep.js\n```\n",
      "utf8",
    );

    assert.deepEqual(
      checkControlCharacters(repoRoot),
      [],
    );
  });

  test("v23-2: tabs and newlines are not control-character findings", (t) => {
    const repoRoot = mkdtempSync(
      join(tmpdir(), "agent-bridge-ctrl-ok-"),
    );
    t.after(() =>
      rmSync(repoRoot, {
        recursive: true,
        force: true,
      }),
    );

    writeFileSync(
      join(repoRoot, "README.md"),
      "# t\n\n\tindented\r\nand CRLF\r\n",
      "utf8",
    );

    assert.deepEqual(
      checkControlCharacters(repoRoot),
      [],
    );
  });

  test("v22-7: the sweep names a loss once and only counts it afterwards", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v22-7 失われた設計文書",
      body: "宛先が来ない",
      toTag: "gone-lane",
      now: Date.now() - TAG_TTL_MS - 60_000,
    });
    bus.close();

    const first = await runTypeScriptProcess(
      SWEEP_ENTRY,
      [],
      userProfile,
    );

    assert.equal(
      first.code,
      0,
      first.stderr,
    );

    /*
     * Two separate processes, because the entry point is where the
     * cursor is read and written. Calling undelivered and the mark by
     * hand leaves that wiring untested, and removing it from the sweep
     * would break nothing.
     */
    assert.ok(
      first.stderr.includes(
        "v22-7 失われた設計文書",
      ),
      first.stderr,
    );
    assert.ok(
      first.stderr.includes(
        "-> gone-lane",
      ),
      first.stderr,
    );
    assert.ok(
      first.stderr.includes(
        "1 undelivered not yet reported",
      ),
      first.stderr,
    );

    const second = await runTypeScriptProcess(
      SWEEP_ENTRY,
      [],
      userProfile,
    );

    assert.equal(
      second.code,
      0,
      second.stderr,
    );
    assert.equal(
      second.stderr.includes(
        "v22-7 失われた設計文書",
      ),
      false,
      second.stderr,
    );
    assert.ok(
      second.stderr.includes(
        "1 undelivered in total",
      ),
      second.stderr,
    );

    /* The sweep records that it ran, separately from how far it read. */
    const after = BridgeBus.open(dbPath);
    try {
      assert.notEqual(
        after.readSweepCompletedAt(),
        null,
      );
      assert.notEqual(
        after.readSweepMark("claude"),
        null,
      );
    } finally {
      after.close();
    }
  });

  test("v23-3: DEL and the C1 range count as control characters", (t) => {
    const repoRoot = mkdtempSync(
      join(tmpdir(), "agent-bridge-ctrl-c1-"),
    );
    t.after(() =>
      rmSync(repoRoot, {
        recursive: true,
        force: true,
      }),
    );

    /*
     * A range that stops at 32 lets DEL and the C1 block through, and
     * they are as invisible as the U+0008 that shipped. Nothing in a
     * viewer distinguishes them from the characters already caught.
     */
    for (const code of [0x7f, 0x80, 0x9f]) {
      writeFileSync(
        join(repoRoot, "README.md"),
        `# t\n\nnode <repo>${String.fromCharCode(
          code,
        )}dist\n`,
        "utf8",
      );

      const findings =
        checkControlCharacters(repoRoot);
      assert.equal(
        findings.length,
        1,
        `U+${code.toString(16)}`,
      );
      assert.ok(
        findings[0]?.detail.includes(
          `U+${code
            .toString(16)
            .padStart(4, "0")}`,
        ),
        JSON.stringify(findings),
      );
    }

    /* Ordinary text above the C1 block stays ordinary. */
    writeFileSync(
      join(repoRoot, "README.md"),
      "# t\n\nカタカナ and ñ and 🙂\n",
      "utf8",
    );
    assert.deepEqual(
      checkControlCharacters(repoRoot),
      [],
    );
  });

  test("v23-4: the control-character check is wired into the run", (t) => {
    const repoRoot = mkdtempSync(
      join(tmpdir(), "agent-bridge-ctrl-wire-"),
    );
    t.after(() =>
      rmSync(repoRoot, {
        recursive: true,
        force: true,
      }),
    );

    execFileSync("git", ["init", "-q"], {
      cwd: repoRoot,
    });
    writeFileSync(
      join(repoRoot, "README.md"),
      `# t\n\nnode <repo>${String.fromCharCode(
        8,
      )}dist\n`,
      "utf8",
    );
    execFileSync(
      "git",
      ["add", "README.md"],
      { cwd: repoRoot },
    );

    /*
     * Owning the logic is not running it. Dropping the call from
     * runDocCheck leaves every direct test passing while the check
     * never fires for anyone who uses the tool.
     */
    const findings = runDocCheck({
      repoRoot,
    });

    assert.ok(
      findings.some(
        (finding) =>
          finding.check ===
          "control-characters",
      ),
      JSON.stringify(findings),
    );
  });

  test("v24-1: one role's cursor cannot carry past the other's unreported loss", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v24-1 claude 宛の損失",
        body: "x",
        toTag: "gone-claude",
        now: T0,
      });
      bus.send({
        fromRole: "claude",
        toRole: "codex",
        subject: "v24-1 codex 宛の損失",
        body: "y",
        toTag: "gone-codex",
        now: T0 + 1_000,
      });

      const after = T0 + TAG_TTL_MS + 10_000;
      bus.recover("claude", after);
      bus.recover("codex", after);

      const codex = bus.undelivered(
        "codex",
        null,
        5,
      );
      assert.equal(codex.lost.length, 1);

      /*
       * With one shared cursor, reporting codex would carry it past the
       * claude bounce that no query had asked about yet, and that loss
       * can never satisfy seq > cursor again. Each role keeps its own.
       */
      bus.writeSweepMark(
        "codex",
        codex.lost[0]!.seq,
        after,
      );

      const claude = bus.undelivered(
        "claude",
        bus.readSweepMark("claude"),
        5,
      );

      assert.deepEqual(
        claude.lost.map(
          (row) => row.subject,
        ),
        ["v24-1 claude 宛の損失"],
      );
    } finally {
      bus.close();
    }
  });

  test("v24-2: the sweep can leave something a person can read", async (t) => {
    const { userProfile, dbPath } =
      makeProfileDb(t);
    const logPath = join(
      userProfile,
      "logs",
      "sweep.log",
    );
    const bus = BridgeBus.open(dbPath);

    bus.send({
      fromRole: "codex",
      toRole: "claude",
      subject: "v24-2 失われた便",
      body: "x",
      toTag: "gone-lane",
      now: Date.now() - TAG_TTL_MS - 60_000,
    });
    bus.close();

    const result = await runTypeScriptProcess(
      SWEEP_ENTRY,
      ["--log", logPath],
      userProfile,
    );

    assert.equal(
      result.code,
      0,
      result.stderr,
    );

    /*
     * Task Scheduler records that a task finished and throws away what
     * it wrote, so a sweep whose only output is stderr runs correctly
     * and leaves nothing behind. The acceptance step asks for the sweep
     * line in a log, which nothing was putting there.
     */
    const written = readFileSync(
      logPath,
      "utf8",
    );

    assert.ok(
      written.includes(
        "agent-bridge sweep db=",
      ),
      written,
    );
    assert.ok(
      written.includes("v24-2 失われた便"),
      written,
    );

    /* Every line is stamped, so a log read later can be placed in time. */
    for (const line of written
      .split(/\r?\n/)
      .filter((line) => line.length > 0)) {
      assert.match(
        line,
        /^\[\d{4}-\d{2}-\d{2}T[\d:.]+Z\] /,
        line,
      );
    }

    /* Appending, so a second run does not discard the first. */
    const again = await runTypeScriptProcess(
      SWEEP_ENTRY,
      ["--log", logPath],
      userProfile,
    );
    assert.equal(again.code, 0, again.stderr);
    assert.ok(
      readFileSync(logPath, "utf8").includes(
        "v24-2 失われた便",
      ),
    );
  });

  test("v24-3: an unusable log argument is refused rather than dropped", async (t) => {
    const { userProfile } = makeProfileDb(t);

    const result = await runTypeScriptProcess(
      SWEEP_ENTRY,
      ["--log"],
      userProfile,
    );

    assert.equal(result.code, 1);
    assert.ok(
      result.stderr.includes(
        "usage: bridge-sweep.js",
      ),
      result.stderr,
    );
  });

  test("v24-4: a sweep that fails says so where the guide says to look", async (t) => {
    const userProfile = mkdtempSync(
      join(tmpdir(), "agent-bridge-nodb-"),
    );
    t.after(() =>
      rmSync(userProfile, {
        recursive: true,
        force: true,
      }),
    );

    const logPath = join(
      userProfile,
      "logs",
      "sweep.log",
    );

    /*
     * No database at all, which is what a wrong path or a wiped profile
     * looks like. Without this the last good sweep stays the newest
     * entry in the log, and a log whose newest line reports success is
     * indistinguishable from a system that is still working.
     */
    const result = await runTypeScriptProcess(
      SWEEP_ENTRY,
      ["--log", logPath],
      userProfile,
    );

    assert.equal(result.code, 1);
    assert.ok(
      result.stderr.includes(
        "agent-bridge sweep failed",
      ),
      result.stderr,
    );

    assert.ok(
      existsSync(logPath),
      "the failure never reached the log",
    );
    assert.ok(
      readFileSync(logPath, "utf8").includes(
        "agent-bridge sweep failed",
      ),
      readFileSync(logPath, "utf8"),
    );
  });

  test("v25-1: the sweep seeks into the event log instead of scanning it", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);
    bus.close();

    const db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
    });

    try {
      const sql = lostQuerySql();

      /*
       * Asked of the statement that actually runs, not a copy of it. The
       * task runs every thirty minutes on a ten-year trigger and nothing
       * prunes events, so a plan that reads SCAN costs more every day it
       * stays. Writing the bound as `@since IS NULL OR e.seq > @since`
       * was enough to lose the rowid seek.
       */
      for (const [shape, text] of Object.entries(
        sql,
      )) {
        const plan = db
          .prepare(
            `EXPLAIN QUERY PLAN ${text}`,
          )
          .all({
            role: "claude",
            since: 0,
            limit: 5,
          }) as Array<{ detail: string }>;

        const details = plan.map(
          (row) => row.detail,
        );

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
  });

  test("v25-2: an absent cursor still reports everything", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.send({
        fromRole: "codex",
        toRole: "claude",
        subject: "v25-2 最初の損失",
        body: "x",
        toTag: "gone-lane",
        now: T0,
      });
      bus.recover(
        "claude",
        T0 + TAG_TTL_MS + 1,
      );

      /*
       * Dropping the nullable branch means null has to arrive as a bound
       * that admits every sequence. Sequences start at 1, so zero does,
       * and a first sweep must not come back empty.
       */
      assert.equal(
        bus.readSweepMark("claude"),
        null,
      );
      assert.equal(
        bus.undelivered(
          "claude",
          bus.readSweepMark("claude"),
          5,
        ).lost.length,
        1,
      );
    } finally {
      bus.close();
    }
  });

  test("v26-1: the completion stamp does not move backwards either", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      bus.writeSweepMark("claude", 5, T0 + 60_000);
      assert.equal(
        bus.readSweepCompletedAt(),
        new Date(T0 + 60_000).toISOString(),
      );

      /*
       * The cursor beside it was already guarded against this. A run that
       * started earlier and finished later would otherwise stamp its own
       * older time, and anything watching for a stopped sweep would read
       * a staleness that never happened.
       */
      bus.writeSweepMark("claude", 6, T0);
      assert.equal(
        bus.readSweepCompletedAt(),
        new Date(T0 + 60_000).toISOString(),
      );

      /* Forward still moves. */
      bus.writeSweepMark(
        "claude",
        7,
        T0 + 120_000,
      );
      assert.equal(
        bus.readSweepCompletedAt(),
        new Date(T0 + 120_000).toISOString(),
      );
    } finally {
      bus.close();
    }
  });

  test("v26-2: a carried page is not described as having happened since", () => {
    const lines = formatUndelivered(
      "claude",
      {
        lost: [
          {
            subject: "carried from an earlier sweep",
            toTag: "lane",
            at: new Date(T0).toISOString(),
            seq: 9,
          },
        ],
        lostSince: 3,
        lostTotal: 12,
      },
      T0 + 60_000,
    );

    const text = lines.join("\n");

    /*
     * The window is a cursor, not a clock. A capped page leaves older
     * losses for the next run, so a heading saying they happened since
     * the previous sweep invites a reader to count them twice.
     */
    assert.ok(
      text.includes(
        "3 undelivered not yet reported",
      ),
      text,
    );
    assert.equal(
      text.includes("since the last sweep"),
      false,
      text,
    );
    assert.ok(
      text.includes(
        "carried to the next sweep",
      ),
      text,
    );
  });

  test("v26-3: the guide only quotes sweep lines the sweep can print", () => {
    const shape =
      /agent-bridge (?:claude|codex) \d+ (.+?)\s*$/;

    /*
     * One report that lights both branches, so the vocabulary comes from
     * the formatter rather than from a list someone maintains. Renaming a
     * heading and leaving the sample in the guide has happened twice, and
     * a sample nobody can reproduce teaches an operator to look for a
     * line that will never appear.
     */
    const emitted = formatUndelivered(
      "claude",
      {
        lost: [
          {
            subject: "s",
            toTag: "lane",
            at: new Date(T0).toISOString(),
            seq: 1,
          },
        ],
        lostSince: 3,
        lostTotal: 12,
      },
      T0 + 60_000,
    );

    const canPrint = new Set(
      emitted
        .map((line) => line.match(shape)?.[1])
        .filter(
          (phrase): phrase is string =>
            phrase !== undefined,
        ),
    );

    assert.ok(
      canPrint.size >= 2,
      [...canPrint].join(" | "),
    );

    const guide = readFileSync(
      join(PROJECT_ROOT, "docs", "deploy.md"),
      "utf8",
    ).replace(/\r\n/g, "\n");

    const quoted = guide
      .split("\n")
      .map((line) => line.match(shape)?.[1])
      .filter(
        (phrase): phrase is string =>
          phrase !== undefined,
      );

    assert.ok(
      quoted.length > 0,
      "the guide quotes no sweep output at all",
    );

    for (const phrase of quoted) {
      assert.ok(
        canPrint.has(phrase),
        `docs/deploy.md quotes "${phrase}", which the sweep cannot print. It can print: ${[
          ...canPrint,
        ].join(" | ")}`,
      );
    }
  });

  test("v27-1: two overlapping sweeps do not both announce the same losses", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      for (let index = 0; index < 3; index += 1) {
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: `v27-1 loss ${index}`,
          body: "x",
          toTag: `gone-${index}`,
          now: T0 + index * 1_000,
        });
      }
      bus.recover(
        "claude",
        T0 + TAG_TTL_MS + 10_000,
      );

      /*
       * Two sweeps ran one second apart in this deployment today, so this
       * is a measured overlap rather than a theoretical one. Reading the
       * cursor and moving it in separate steps let both runs take the same
       * page, and the second announced rows the first had already named.
       */
      const first = bus.reserveLosses(
        "claude",
        5,
      );
      const second = bus.reserveLosses(
        "claude",
        5,
      );

      assert.equal(first.lost.length, 3);
      assert.deepEqual(second.lost, []);
      assert.equal(second.lostSince, 0);

      /* The total is a property of the queue, not of who reported it. */
      assert.equal(first.lostTotal, 3);
      assert.equal(second.lostTotal, 3);
    } finally {
      bus.close();
    }
  });

  test("v27-2: a reserved page stops at the cap and the next run continues", (t) => {
    const { dbPath } = makeDb(t);
    const bus = BridgeBus.open(dbPath);

    try {
      for (let index = 0; index < 7; index += 1) {
        bus.send({
          fromRole: "codex",
          toRole: "claude",
          subject: `v27-2 loss ${index}`,
          body: "x",
          toTag: `gone-${index}`,
          now: T0 + index * 1_000,
        });
      }
      bus.recover(
        "claude",
        T0 + TAG_TTL_MS + 10_000,
      );

      const first = bus.reserveLosses(
        "claude",
        5,
      );
      const second = bus.reserveLosses(
        "claude",
        5,
      );

      assert.deepEqual(
        first.lost.map((row) => row.subject),
        [0, 1, 2, 3, 4].map(
          (index) => `v27-2 loss ${index}`,
        ),
      );
      assert.equal(first.lostSince, 7);

      /*
       * Reserving must not swallow the remainder. The cap is a page, and
       * the run after it picks up where the page stopped.
       */
      assert.deepEqual(
        second.lost.map((row) => row.subject),
        ["v27-2 loss 5", "v27-2 loss 6"],
      );
    } finally {
      bus.close();
    }
  });
}
