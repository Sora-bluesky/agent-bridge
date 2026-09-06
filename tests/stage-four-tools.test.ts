import assert from "node:assert/strict";
import {
  type ChildProcessWithoutNullStreams,
  spawn,
} from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import {
  basename,
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
  BridgeBus,
  type EndpointMapping,
  MIGRATION_LOCK_KEY,
  MIGRATION_PAUSE_ENV,
  SCHEMA_VERSION,
  computeEnvelopeHash,
  initializeBridgeDatabaseAtPath,
  migrateBridgeDatabaseAtPath,
  readMigrationLockAtPath,
  sha256,
} from "../src/db.js";
import {
  type ProcessScanResult,
  runMigrationPrecheckAtPath,
} from "../src/bridge-init.js";

const PROJECT_ROOT = resolve(
  fileURLToPath(new URL("..", import.meta.url)),
);
const INIT_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-init.ts",
);
const HOOK_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "hook-notify.ts",
);
const SWEEP_ENTRY = join(
  PROJECT_ROOT,
  "src",
  "bridge-sweep.ts",
);
const CREATED_AT =
  "2026-09-06T00:00:00.000Z";

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

const VALID_MAPPING: EndpointMapping = {
  endpoints: [
    {
      role: "claude",
      name: "claude-main",
    },
    {
      role: "codex",
      name: "codex-main",
    },
  ],
  tags: [
    {
      role: "claude",
      tag: null,
      endpoint: "claude-main",
    },
    {
      role: "codex",
      tag: null,
      endpoint: "codex-main",
    },
    {
      role: "codex",
      tag: "lane",
      endpoint: "codex-main",
    },
  ],
};

const CLEAN_CONFIG = JSON.stringify({
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
  },
  env: {
    AGENT_BRIDGE_ENDPOINT: "claude-main",
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
      },
    ],
  },
});

interface ProcessResult {
  code: number | null;
  stdout: string;
  stderr: string;
}

function sqlLiteral(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

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

function writeV41Db(dbPath: string): void {
  mkdirSync(dirname(dbPath), {
    recursive: true,
  });
  const db = new Database(dbPath);
  try {
    db.pragma("journal_mode = WAL");
    db.exec(V41_SCHEMA_SQL);
    const insert = db.prepare(
      "INSERT INTO meta (k, v) VALUES (?, ?)",
    );
    insert.run("root_id", randomUUID());
    insert.run("schema_version", "4.1");
    insert.run("created_at", CREATED_AT);
  } finally {
    db.close();
  }
}

function readMeta(
  dbPath: string,
  key: string,
): string | null {
  return withDb(dbPath, (db) => {
    const row = db
      .prepare(
        "SELECT v FROM meta WHERE k = ?",
      )
      .get(key) as
      | { v: string }
      | undefined;
    return row?.v ?? null;
  });
}

function tableColumns(
  dbPath: string,
  table: string,
): string[] {
  return withDb(
    dbPath,
    (db) =>
      (
        db.pragma(
          `table_info(${table})`,
        ) as Array<{ name: string }>
      ).map((row) => row.name),
  );
}

function backupFiles(
  dbPath: string,
): string[] {
  const prefix = `${basename(dbPath)}.pre-`;
  return readdirSync(dirname(dbPath))
    .filter((name) =>
      name.startsWith(prefix),
    )
    .sort()
    .map((name) =>
      join(dirname(dbPath), name),
    );
}

function integrity(
  dbPath: string,
): string {
  return withDb(dbPath, (db) =>
    String(
      db.pragma("integrity_check", {
        simple: true,
      }),
    ),
  );
}

function makeBackup(
  dbPath: string,
): string {
  const backupPath = `${dbPath}.pre-fixture`;
  withDb(dbPath, (db) => {
    db.exec(
      `VACUUM INTO ${sqlLiteral(
        backupPath,
      )}`,
    );
  });
  assert.equal(integrity(backupPath), "ok");
  return backupPath;
}

async function runEntry(
  userProfile: string,
  entry: string,
  args: readonly string[],
  stdin = "",
  extraEnv: NodeJS.ProcessEnv = {},
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
        ...extraEnv,
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

async function startPausedMigration(
  userProfile: string,
): Promise<ChildProcessWithoutNullStreams> {
  const child = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      INIT_ENTRY,
      "--migrate",
    ],
    {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        USERPROFILE: userProfile,
        [MIGRATION_PAUSE_ENV]: "1",
      },
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  child.stdin.end();
  child.stdout.resume();
  child.stderr.setEncoding("utf8");

  await new Promise<void>(
    (resolvePause, rejectPause) => {
      let stderr = "";

      const onData = (chunk: string): void => {
        stderr += chunk;
        if (
          stderr.includes(
            "migration paused after destructive DDL",
          )
        ) {
          cleanup();
          child.stderr.resume();
          resolvePause();
        }
      };
      const onClose = (
        code: number | null,
      ): void => {
        cleanup();
        rejectPause(
          new Error(
            `migration exited before pause: code=${code} stderr=${stderr}`,
          ),
        );
      };
      const cleanup = (): void => {
        child.stderr.off("data", onData);
        child.off("close", onClose);
      };

      child.stderr.on("data", onData);
      child.once("close", onClose);
    },
  );

  return child;
}

async function killPaused(
  child: ChildProcessWithoutNullStreams,
): Promise<void> {
  const closed = once(child, "close");
  child.kill();
  await closed;
}

function writeMapping(
  directory: string,
  name: string,
  value: unknown,
): string {
  const path = join(directory, name);
  writeFileSync(
    path,
    JSON.stringify(value),
    "utf8",
  );
  return path;
}

function seedCurrentDelivery(
  dbPath: string,
  options: {
    live?: boolean;
    unresolvedTag?: string;
  },
): void {
  withDb(dbPath, (db) => {
    const sourceId = randomUUID();
    const targetId = randomUUID();
    const messageId = randomUUID();
    const attemptId = randomUUID();
    const subject = "stage four precheck";
    const body = "fixture body";

    const insertEndpoint = db.prepare(
      `INSERT INTO endpoints (
         endpoint_id, role, name, created_at
       ) VALUES (?, ?, ?, ?)`,
    );
    insertEndpoint.run(
      sourceId,
      "claude",
      "claude-main",
      CREATED_AT,
    );
    insertEndpoint.run(
      targetId,
      "codex",
      "codex-main",
      CREATED_AT,
    );

    db.prepare(
      `INSERT INTO messages (
         message_id, from_role, to_role,
         from_tag, subject, body,
         envelope_sha256, envelope_version,
         body_sha256, status, sent_at,
         source_endpoint_id, legacy_to_tag
       ) VALUES (
         ?, 'claude', 'codex',
         NULL, ?, ?,
         ?, 2,
         ?, 'stored', ?,
         ?, ?
       )`,
    ).run(
      messageId,
      subject,
      body,
      computeEnvelopeHash(
        "claude",
        subject,
        body,
      ),
      sha256(body),
      CREATED_AT,
      sourceId,
      options.unresolvedTag ?? null,
    );

    if (options.live) {
      db.prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state,
           holder, attempt_id, attempt_count,
           lease_until
         ) VALUES (?, ?, 'leased', ?, ?, 1, ?)`,
      ).run(
        messageId,
        targetId,
        "codex:test",
        attemptId,
        Date.now() + 60_000,
      );
    } else {
      db.prepare(
        `INSERT INTO deliveries (
           message_id, endpoint_id, state
         ) VALUES (?, NULL, 'pending')`,
      ).run(messageId);
    }
  });
}

function quietScan(): ProcessScanResult {
  return {
    available: true,
    running: 0,
    detail: "test process list",
  };
}

function checkLine(
  lines: readonly string[],
  id: string,
): string {
  const line = lines.find((candidate) =>
    candidate.startsWith(
      `precheck ${id}:`,
    ),
  );
  return assert.ok(line), line;
}

function assertOnlyFailure(
  lines: readonly string[],
  failedId: string,
): void {
  for (const id of [
    "1",
    "2a",
    "2b",
    "3",
    "4",
  ]) {
    if (id === failedId) {
      assert.match(
        checkLine(lines, id),
        /: NG /,
      );
    } else {
      assert.match(
        checkLine(lines, id),
        /: OK /,
      );
    }
  }
}

function makePrecheckFixture(
  t: TestContext,
  prefix: string,
): {
  userProfile: string;
  dbPath: string;
  configPath: string;
} {
  const fixture = makeProfile(t, prefix);
  initializeBridgeDatabaseAtPath(
    fixture.dbPath,
  );
  const configPath = join(
    fixture.userProfile,
    "operator-config.json",
  );
  writeFileSync(
    configPath,
    CLEAN_CONFIG,
    "utf8",
  );
  return {
    ...fixture,
    configPath,
  };
}

test(
  "v42-1: migration creates and verifies the pre-version backup before changing the database",
  (t) => {
    const success = makeProfile(
      t,
      "agent-bridge-v42-1-success-",
    );
    writeV41Db(success.dbPath);

    const metadata =
      migrateBridgeDatabaseAtPath(
        success.dbPath,
      );

    assert.equal(
      metadata.schemaVersion,
      SCHEMA_VERSION,
    );
    assert.match(
      metadata.backupPath,
      /\.pre-4\.1-\d{8}-\d{6}$/,
    );
    assert.equal(
      integrity(metadata.backupPath),
      "ok",
    );
    assert.equal(
      readMeta(
        metadata.backupPath,
        "schema_version",
      ),
      "4.1",
    );
    assert.equal(
      readMeta(
        success.dbPath,
        "schema_version",
      ),
      SCHEMA_VERSION,
    );

    const collision = makeProfile(
      t,
      "agent-bridge-v42-1-collision-",
    );
    writeV41Db(collision.dbPath);
    const before = readFileSync(
      collision.dbPath,
    );
    const RealDate = Date;
    const fixedMs = RealDate.parse(
      "2026-09-06T01:02:03.000Z",
    );

    class FixedDate extends RealDate {
      constructor(value?: string | number) {
        super(value ?? fixedMs);
      }

      static now(): number {
        return fixedMs;
      }
    }

    const collisionPath =
      `${collision.dbPath}.pre-4.1-20260906-010203`;
    writeFileSync(
      collisionPath,
      "occupied",
      "utf8",
    );

    try {
      globalThis.Date =
        FixedDate as unknown as DateConstructor;
      assert.throws(
        () =>
          migrateBridgeDatabaseAtPath(
            collision.dbPath,
          ),
        /already exists|output file/i,
      );
    } finally {
      globalThis.Date = RealDate;
    }

    assert.deepEqual(
      readFileSync(collision.dbPath),
      before,
    );
    assert.equal(
      readMeta(
        collision.dbPath,
        "schema_version",
      ),
      "4.1",
    );
    assert.equal(
      readMigrationLockAtPath(
        collision.dbPath,
      ),
      null,
    );
  },
);

test(
  "v42-2: the migration lock blocks the server and silences the hook while the sweep reports one skip line",
  async (t) => {
    const paused = makeProfile(
      t,
      "agent-bridge-v42-2-paused-",
    );
    writeV41Db(paused.dbPath);
    const child =
      await startPausedMigration(
        paused.userProfile,
      );
    t.after(() => {
      if (child.exitCode === null) {
        child.kill();
      }
    });

    const lock =
      readMigrationLockAtPath(
        paused.dbPath,
      );
    assert.ok(lock);

    assert.throws(
      () => BridgeBus.open(paused.dbPath),
      new RegExp(
        `migration in progress since ${lock.started_at} pid=${lock.pid}`,
      ),
    );

    const hook = await runEntry(
      paused.userProfile,
      HOOK_ENTRY,
      ["--event", "stop"],
      "{}",
    );
    assert.equal(hook.code, 0);
    assert.equal(hook.stdout, "");
    assert.equal(hook.stderr, "");

    const sweep = await runEntry(
      paused.userProfile,
      SWEEP_ENTRY,
      [],
    );
    assert.equal(sweep.code, 0);
    assert.equal(sweep.stdout, "");
    assert.equal(
      sweep.stderr,
      `agent-bridge sweep skipped: migration in progress since ${lock.started_at} pid=${lock.pid}\n`,
    );

    await killPaused(child);

    const completed = makeProfile(
      t,
      "agent-bridge-v42-2-completed-",
    );
    writeV41Db(completed.dbPath);
    migrateBridgeDatabaseAtPath(
      completed.dbPath,
    );
    assert.equal(
      readMigrationLockAtPath(
        completed.dbPath,
      ),
      null,
    );
  },
);

test(
  "v42-3: a leftover lock is never removed automatically and names its owner",
  (t) => {
    const fixture = makeProfile(
      t,
      "agent-bridge-v42-3-",
    );
    initializeBridgeDatabaseAtPath(
      fixture.dbPath,
    );
    const lock = {
      pid: 4242,
      started_at:
        "2026-09-06T02:03:04.000Z",
    };

    withDb(fixture.dbPath, (db) => {
      db.prepare(
        "INSERT INTO meta (k, v) VALUES (?, ?)",
      ).run(
        MIGRATION_LOCK_KEY,
        JSON.stringify(lock),
      );
    });

    assert.throws(
      () =>
        migrateBridgeDatabaseAtPath(
          fixture.dbPath,
        ),
      /previous migration in progress since 2026-09-06T02:03:04\.000Z pid=4242; restore from the backup/,
    );
    assert.deepEqual(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      lock,
    );
    assert.throws(
      () => BridgeBus.open(fixture.dbPath),
      /migration in progress since 2026-09-06T02:03:04\.000Z pid=4242/,
    );
  },
);

test(
  "v42-4: mapping intake distinguishes shape, missing endpoint, and role mismatch without writing a current database",
  async (t) => {
    const fixture = makeProfile(
      t,
      "agent-bridge-v42-4-",
    );
    initializeBridgeDatabaseAtPath(
      fixture.dbPath,
    );

    const malformed = [
      {
        name: "shape.json",
        value: {
          endpoints: "not-an-array",
          tags: [],
        },
        message:
          /mapping shape is invalid/,
      },
      {
        name: "missing.json",
        value: {
          endpoints: [],
          tags: [
            {
              role: "codex",
              tag: null,
              endpoint: "missing",
            },
          ],
        },
        message:
          /mapping tag endpoint is not declared/,
      },
      {
        name: "role.json",
        value: {
          endpoints: [
            {
              role: "claude",
              name: "desk",
            },
          ],
          tags: [
            {
              role: "codex",
              tag: null,
              endpoint: "desk",
            },
          ],
        },
        message:
          /mapping tag endpoint role does not match/,
      },
    ];

    for (const candidate of malformed) {
      const mappingPath = writeMapping(
        fixture.userProfile,
        candidate.name,
        candidate.value,
      );
      const result = await runEntry(
        fixture.userProfile,
        INIT_ENTRY,
        [
          "--migrate",
          "--mapping",
          mappingPath,
        ],
      );
      assert.equal(result.code, 1);
      assert.match(
        result.stderr,
        candidate.message,
      );
    }

    const validPath = writeMapping(
      fixture.userProfile,
      "valid.json",
      VALID_MAPPING,
    );
    const before = readFileSync(
      fixture.dbPath,
    );
    const valid = await runEntry(
      fixture.userProfile,
      INIT_ENTRY,
      [
        "--migrate",
        "--mapping",
        validPath,
      ],
    );

    assert.equal(valid.code, 1);
    assert.match(
      valid.stderr,
      /there is nothing to migrate/,
    );
    assert.doesNotMatch(
      valid.stderr,
      /mapping .*invalid|mapping tag endpoint/,
    );
    assert.deepEqual(
      readFileSync(fixture.dbPath),
      before,
    );
    assert.deepEqual(
      backupFiles(fixture.dbPath),
      [],
    );
    assert.equal(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      null,
    );
  },
);

test(
  "v42-5: precheck reports each independent failure, unknown configs, and ignores adjacent repository documentation",
  async (t) => {
    const passing = makePrecheckFixture(
      t,
      "agent-bridge-v42-5-pass-",
    );
    makeBackup(passing.dbPath);
    const pass =
      runMigrationPrecheckAtPath(
        passing.dbPath,
        VALID_MAPPING,
        [passing.configPath],
        quietScan,
      );
    assert.equal(pass.passed, true);
    for (const id of [
      "1",
      "2a",
      "2b",
      "3",
      "4",
    ]) {
      assert.match(
        checkLine(pass.lines, id),
        /: OK /,
      );
    }
    assert.match(
      checkLine(pass.lines, "1b"),
      /: 対象外 /,
    );

    const live = makePrecheckFixture(
      t,
      "agent-bridge-v42-5-live-",
    );
    seedCurrentDelivery(live.dbPath, {
      live: true,
    });
    makeBackup(live.dbPath);
    const liveReport =
      runMigrationPrecheckAtPath(
        live.dbPath,
        VALID_MAPPING,
        [live.configPath],
        quietScan,
      );
    assertOnlyFailure(
      liveReport.lines,
      "1",
    );

    const retired = makePrecheckFixture(
      t,
      "agent-bridge-v42-5-retired-",
    );
    writeFileSync(
      retired.configPath,
      `${CLEAN_CONFIG}\nto_tag\n`,
      "utf8",
    );
    makeBackup(retired.dbPath);
    const retiredReport =
      runMigrationPrecheckAtPath(
        retired.dbPath,
        VALID_MAPPING,
        [retired.configPath],
        quietScan,
      );
    assertOnlyFailure(
      retiredReport.lines,
      "2a",
    );

    const missingEndpoint =
      makePrecheckFixture(
        t,
        "agent-bridge-v42-5-endpoint-",
      );
    writeFileSync(
      missingEndpoint.configPath,
      JSON.stringify({
        server: {
          args: [
            "C:/agent-bridge/dist/server.js",
            "--role",
            "codex",
          ],
        },
        hook: {
          args: [
            "C:/agent-bridge/dist/hook-notify.js",
          ],
        },
        env: {
          AGENT_BRIDGE_ENDPOINT:
            "claude-main",
        },
      }),
      "utf8",
    );
    makeBackup(missingEndpoint.dbPath);
    const endpointReport =
      runMigrationPrecheckAtPath(
        missingEndpoint.dbPath,
        VALID_MAPPING,
        [missingEndpoint.configPath],
        quietScan,
      );
    assertOnlyFailure(
      endpointReport.lines,
      "2b",
    );

    const unresolved =
      makePrecheckFixture(
        t,
        "agent-bridge-v42-5-unresolved-",
      );
    seedCurrentDelivery(
      unresolved.dbPath,
      {
        unresolvedTag: "not-mapped",
      },
    );
    makeBackup(unresolved.dbPath);
    const unresolvedReport =
      runMigrationPrecheckAtPath(
        unresolved.dbPath,
        VALID_MAPPING,
        [unresolved.configPath],
        quietScan,
      );
    assertOnlyFailure(
      unresolvedReport.lines,
      "3",
    );
    assert.match(
      checkLine(
        unresolvedReport.lines,
        "3",
      ),
      /unresolved=1\b/,
    );

    const noBackup = makePrecheckFixture(
      t,
      "agent-bridge-v42-5-backup-",
    );
    const backupReport =
      runMigrationPrecheckAtPath(
        noBackup.dbPath,
        VALID_MAPPING,
        [noBackup.configPath],
        quietScan,
      );
    assertOnlyFailure(
      backupReport.lines,
      "4",
    );

    const unreadable =
      makePrecheckFixture(
        t,
        "agent-bridge-v42-5-unreadable-",
      );
    makeBackup(unreadable.dbPath);
    const unreadableReport =
      runMigrationPrecheckAtPath(
        unreadable.dbPath,
        VALID_MAPPING,
        [
          join(
            unreadable.userProfile,
            "does-not-exist.json",
          ),
        ],
        quietScan,
      );
    assert.equal(
      unreadableReport.passed,
      false,
    );
    assert.match(
      checkLine(
        unreadableReport.lines,
        "2a",
      ),
      /: 未確認 /,
    );
    assert.match(
      checkLine(
        unreadableReport.lines,
        "2b",
      ),
      /: 未確認 /,
    );

    const noConfig =
      makePrecheckFixture(
        t,
        "agent-bridge-v42-5-no-config-",
      );
    makeBackup(noConfig.dbPath);
    const noConfigReport =
      runMigrationPrecheckAtPath(
        noConfig.dbPath,
        VALID_MAPPING,
        [],
        quietScan,
      );
    assert.equal(
      noConfigReport.passed,
      false,
    );
    assert.match(
      checkLine(
        noConfigReport.lines,
        "2a",
      ),
      /: 未確認 /,
    );
    assert.match(
      checkLine(
        noConfigReport.lines,
        "2b",
      ),
      /: 未確認 /,
    );

    copyFileSync(
      join(
        PROJECT_ROOT,
        "docs",
        "deploy.md",
      ),
      join(
        dirname(passing.dbPath),
        "deploy.md",
      ),
    );
    const docsIgnored =
      runMigrationPrecheckAtPath(
        passing.dbPath,
        VALID_MAPPING,
        [passing.configPath],
        quietScan,
      );
    assert.equal(
      docsIgnored.passed,
      true,
    );
    assert.match(
      checkLine(
        docsIgnored.lines,
        "2a",
      ),
      /: OK retired_identifiers=0$/,
    );

    const cliMapping = writeMapping(
      noConfig.userProfile,
      "mapping.json",
      VALID_MAPPING,
    );
    const cliNoConfig = await runEntry(
      noConfig.userProfile,
      INIT_ENTRY,
      [
        "--precheck",
        "--mapping",
        cliMapping,
      ],
    );
    assert.equal(cliNoConfig.code, 1);
    assert.match(
      cliNoConfig.stderr,
      /precheck 2a: 未確認/,
    );
    assert.match(
      cliNoConfig.stderr,
      /precheck 2b: 未確認/,
    );
  },
);

test(
  "v42-6: forced termination leaves the lock and old schema, while restoring the pre-lock backup permits a complete retry",
  async (t) => {
    const fixture = makeProfile(
      t,
      "agent-bridge-v42-6-",
    );
    writeV41Db(fixture.dbPath);
    const originalColumns =
      tableColumns(
        fixture.dbPath,
        "messages",
      );
    const child =
      await startPausedMigration(
        fixture.userProfile,
      );
    t.after(() => {
      if (child.exitCode === null) {
        child.kill();
      }
    });

    const lock =
      readMigrationLockAtPath(
        fixture.dbPath,
      );
    assert.ok(lock);
    const backups =
      backupFiles(fixture.dbPath);
    assert.equal(backups.length, 1);
    const backupPath = backups[0]!;

    await killPaused(child);

    assert.equal(
      readMeta(
        fixture.dbPath,
        "schema_version",
      ),
      "4.1",
    );
    assert.deepEqual(
      tableColumns(
        fixture.dbPath,
        "messages",
      ),
      originalColumns,
    );
    assert.deepEqual(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      lock,
    );

    const refused = await runEntry(
      fixture.userProfile,
      INIT_ENTRY,
      ["--migrate"],
    );
    assert.equal(refused.code, 1);
    assert.match(
      refused.stderr,
      new RegExp(
        `migration in progress since ${lock.started_at} pid=${lock.pid}`,
      ),
    );
    assert.match(
      refused.stderr,
      /restore from the backup/,
    );
    assert.deepEqual(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      lock,
    );

    rmSync(`${fixture.dbPath}-wal`, {
      force: true,
    });
    rmSync(`${fixture.dbPath}-shm`, {
      force: true,
    });
    copyFileSync(
      backupPath,
      fixture.dbPath,
    );

    assert.equal(
      readMeta(
        fixture.dbPath,
        "schema_version",
      ),
      "4.1",
    );
    assert.equal(
      integrity(fixture.dbPath),
      "ok",
    );
    assert.equal(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      null,
    );

    rmSync(backupPath, {
      force: true,
    });
    const retried = await runEntry(
      fixture.userProfile,
      INIT_ENTRY,
      ["--migrate"],
    );
    assert.equal(
      retried.code,
      0,
      retried.stderr,
    );
    assert.match(
      retried.stderr,
      /schema_version=4\.10/,
    );
    assert.match(
      retried.stderr,
      / backup=/,
    );
    assert.equal(
      readMeta(
        fixture.dbPath,
        "schema_version",
      ),
      SCHEMA_VERSION,
    );
    assert.equal(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      null,
    );

    const bus =
      BridgeBus.open(fixture.dbPath);
    bus.close();
  },
);

test(
  "v42-7: E-4a leaves the public schema and ordinary open behavior unchanged",
  (t) => {
    const fixture = makeProfile(
      t,
      "agent-bridge-v42-7-",
    );
    writeV41Db(fixture.dbPath);

    const metadata =
      migrateBridgeDatabaseAtPath(
        fixture.dbPath,
      );
    assert.equal(
      metadata.schemaVersion,
      "4.10",
    );
    assert.equal(
      readMeta(
        fixture.dbPath,
        "schema_version",
      ),
      "4.10",
    );
    assert.equal(
      readMigrationLockAtPath(
        fixture.dbPath,
      ),
      null,
    );

    const bus =
      BridgeBus.open(fixture.dbPath);
    try {
      assert.equal(
        bus.metadata.schemaVersion,
        "4.10",
      );
    } finally {
      bus.close();
    }
  },
);