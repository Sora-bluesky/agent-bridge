import { execFileSync } from "node:child_process";
import {
  readFileSync,
  readdirSync,
  statSync,
} from "node:fs";
import {
  basename,
  dirname,
  join,
  resolve,
} from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import { parse as parseToml } from "smol-toml";
import {
  BUSY_TIMEOUT_MS,
  BridgeBus,
  type EndpointMapping,
  formatMigrationLock,
  getBridgeDbPath,
  readMigrationLockAtPath,
  initializeFixedBridgeDatabase,
  MIGRATION_PAUSE_ENV,
  migrateFixedBridgeDatabase,
  PRESENTED_TTL_MS,
  SCHEMA_VERSION,
  type Role,
  validateEndpointMapping,
} from "./db.js";
import {
  errorMessage,
  quoteForOneField,
  writeErrorRecord,
} from "./one-line.js";
import { parseEvent } from "./hook-notify.js";
import { parseStartupArguments } from "./server.js";

const RETIRED_IDENTIFIERS = [
  "bridge_hello",
  "to_tag",
  "from_tag",
  "broadcast",
  "on_timeout",
  "require_tag",
  "strict_addressing",
  "to_endpoint",
] as const;

const RETIRED_PATTERN = new RegExp(
  `\\b(?:${RETIRED_IDENTIFIERS.join("|")})\\b`,
  "g",
);
const SERVER_ENTRY_PATTERN =
  /(?:^|[\\/\s"'])server\.(?:js|ts)(?:["'\s]|$)/gi;
const HOOK_ENTRY_PATTERN =
  /(?:^|[\\/\s"'])hook-notify\.(?:js|ts)(?:["'\s]|$)/gi;
const AGENT_BRIDGE_SERVER_OPTION_PATTERN =
  /(?:^|[\s"'])--(?:role["']?(?:\s+|=)["']?(?:claude|codex)(?:["'\s]|$)|endpoint(?:["'\s=]|$))/i;

interface ConfigRead {
  path: string;
  content: string | null;
  parsed: unknown | null;
}

interface ConfigRegistration {
  command: string;
  args: string[];
  env: Record<string, unknown>;
}

interface ConfigRegistrations {
  servers: ConfigRegistration[];
  hooks: ConfigRegistration[];
}

function isRecord(
  value: unknown,
): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function patternMatches(
  pattern: RegExp,
  content: string,
): boolean {
  pattern.lastIndex = 0;
  const matched = pattern.test(content);
  pattern.lastIndex = 0;
  return matched;
}

function configRegistration(
  entry: Record<string, unknown>,
): ConfigRegistration {
  return {
    command:
      typeof entry.command === "string"
        ? entry.command
        : "",
    args: Array.isArray(entry.args)
      ? entry.args.filter(
          (argument): argument is string =>
            typeof argument === "string",
        )
      : [],
    env: isRecord(entry.env)
      ? entry.env
      : {},
  };
}

function registrationMatches(
  registration: ConfigRegistration,
  pattern: RegExp,
): boolean {
  return (
    registration.command.length > 0 &&
    [
      registration.command,
      ...registration.args,
    ].some((argument) =>
      patternMatches(pattern, argument),
    )
  );
}

function configRegistrations(
  parsed: unknown,
): ConfigRegistrations {
  const registrations: ConfigRegistrations = {
    servers: [],
    hooks: [],
  };

  /*
   * The documented hook registration (docs/deploy.md section 4) keeps
   * the environment in the settings file's top-level `env`, which Claude
   * Code applies to every hook process. A hook entry rarely carries its
   * own `env`; when it does, its values win.
   */
  const fileEnvironment =
    isRecord(parsed) && isRecord(parsed.env)
      ? parsed.env
      : {};

  /*
   * JSON.parse and parseToml both return ordinary object graphs. Walk
   * either format by context: registrations may live at the top level
   * or inside project-scoped blocks. Container keys only switch a null
   * context, so a server named "hooks" remains a server. Once an object
   * matches the entry script for its context, do not descend into it.
   */
  const visit = (
    value: unknown,
    context: "servers" | "hooks" | null,
  ): void => {
    if (Array.isArray(value)) {
      for (const item of value) {
        visit(item, context);
      }
      return;
    }

    if (!isRecord(value)) {
      return;
    }

    const registration =
      configRegistration(value);
    if (
      context === "servers" &&
      registrationMatches(
        registration,
        SERVER_ENTRY_PATTERN,
      )
    ) {
      registrations.servers.push(
        registration,
      );
      return;
    }
    if (
      context === "hooks" &&
      registrationMatches(
        registration,
        HOOK_ENTRY_PATTERN,
      )
    ) {
      registrations.hooks.push({
        ...registration,
        env: {
          ...fileEnvironment,
          ...registration.env,
        },
      });
      return;
    }

    for (const [key, nested] of Object.entries(
      value,
    )) {
      visit(
        nested,
        context !== null
          ? context
          : key === "mcpServers" ||
              key === "mcp_servers"
            ? "servers"
            : key === "hooks"
              ? "hooks"
              : null,
      );
    }
  };

  visit(parsed, null);
  return registrations;
}

/*
 * Claude Code runs a hook in one of two forms (docs: hooks, "Exec form
 * and shell form"): with `args`, `command` is spawned directly with that
 * vector; without `args`, `command` is one string handed to a shell. The
 * shell itself cannot be asked here, so its word split is approximated:
 * whitespace separates words, double or single quotes group them. That
 * is the one place this check does not run the real parser.
 */
function shellWords(command: string): string[] {
  const words: string[] = [];
  let word = "";
  let quote: '"' | "'" | null = null;
  let inWord = false;
  for (const character of command) {
    if (quote !== null) {
      if (character === quote) {
        quote = null;
      } else {
        word += character;
      }
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      inWord = true;
      continue;
    }
    if (/\s/.test(character)) {
      if (inWord) {
        words.push(word);
        word = "";
        inWord = false;
      }
      continue;
    }
    word += character;
    inWord = true;
  }
  if (inWord) {
    words.push(word);
  }
  return words;
}

function argumentsAfterEntry(
  registration: ConfigRegistration,
  pattern: RegExp,
): string[] {
  const invocation =
    registration.args.length === 0
      ? shellWords(registration.command)
      : [registration.command, ...registration.args];
  const entryIndex =
    invocation.findIndex((argument) =>
      patternMatches(pattern, argument),
    );

  return entryIndex < 0
    ? []
    : invocation.slice(entryIndex + 1);
}

export interface ProcessScanResult {
  available: boolean;
  running: number;
  detail: string;
}

export interface MigrationPrecheckReport {
  passed: boolean;
  lines: string[];
}

type ProcessScanner = () => ProcessScanResult;

function isDirectExecution(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }

  return (
    pathToFileURL(resolve(entry)).href ===
    import.meta.url
  );
}

function loadMapping(
  path: string,
): EndpointMapping {
  let raw: string;
  try {
    raw = readFileSync(
      resolve(path),
      "utf8",
    );
  } catch {
    throw new Error(
      "mapping file could not be read",
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error(
      "mapping shape is invalid: JSON could not be parsed",
    );
  }

  return validateEndpointMapping(parsed);
}

function readConfigs(
  paths: readonly string[],
): {
  files: ConfigRead[];
  unreadable: number;
} {
  let unreadable = 0;
  const files = paths.map((path): ConfigRead => {
    const absolute = resolve(path);
    try {
      const content = readFileSync(
        absolute,
        "utf8",
      );
      /*
       * The extension is what the applications themselves go by:
       * Claude Code reads .json, Codex reads .toml. Either kind that does
       * not parse is a file the application cannot load, so it counts as
       * unreadable. Anything else (the AGENTS.md transcript that check
       * 2a covers) is text: scanned for retired words, never a source of
       * registrations.
       */
      const extension = absolute
        .toLowerCase()
        .replace(/^.*\./, ".");
      const parsed =
        extension === ".json"
          ? JSON.parse(content)
          : extension === ".toml"
            ? parseToml(content)
            : null;

      return {
        path: absolute,
        content,
        parsed,
      };
    } catch {
      unreadable += 1;
      return {
        path: absolute,
        content: null,
        parsed: null,
      };
    }
  });

  return { files, unreadable };
}

function isAgentBridgeServerCommand(
  commandLine: string,
): boolean {
  return (
    patternMatches(
      SERVER_ENTRY_PATTERN,
      commandLine,
    ) &&
    AGENT_BRIDGE_SERVER_OPTION_PATTERN.test(
      commandLine,
    )
  );
}

export function defaultProcessScan(): ProcessScanResult {
  try {
    if (process.platform === "win32") {
      const output = execFileSync(
        "powershell.exe",
        [
          "-NoProfile",
          "-NonInteractive",
          "-Command",
          /*
           * PowerShell writes its console code page (cp932 here) unless
           * told otherwise, and a command line holding Japanese then
           * arrives as bytes Node cannot decode as UTF-8, which breaks
           * the JSON escapes. Measured on this machine: a Codex prompt
           * passed as an argument made every scan "unavailable".
           */
          "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress",
        ],
        {
          encoding: "utf8",
          windowsHide: true,
        },
      ).trim();
      const parsed: unknown =
        output.length === 0
          ? []
          : JSON.parse(output);
      const rows = (
        Array.isArray(parsed)
          ? parsed
          : [parsed]
      ) as Array<Record<string, unknown>>;
      let running = 0;

      for (const row of rows) {
        const pid = row.ProcessId;
        const commandLine = row.CommandLine;
        if (
          typeof pid === "number" &&
          pid !== process.pid &&
          typeof commandLine === "string" &&
          isAgentBridgeServerCommand(
            commandLine,
          )
        ) {
          running += 1;
        }
      }

      return {
        available: true,
        running,
        detail: "PowerShell process list",
      };
    }

    const output = execFileSync(
      "ps",
      ["-eo", "pid=,args="],
      { encoding: "utf8" },
    );
    let running = 0;

    for (const line of output.split(/\r?\n/)) {
      const match = /^\s*(\d+)\s+(.*)$/.exec(
        line,
      );
      if (!match) {
        continue;
      }

      const pid = Number(match[1]);
      const commandLine = match[2] ?? "";
      if (
        pid !== process.pid &&
        isAgentBridgeServerCommand(
          commandLine,
        )
      ) {
        running += 1;
      }
    }

    return {
      available: true,
      running,
      detail: "ps process list",
    };
  } catch (error) {
    return {
      available: false,
      running: 0,
      detail: `process list unavailable: ${errorMessage(
        error,
      )}`,
    };
  }
}

function mappingResolves(
  mapping: EndpointMapping,
  role: Role,
  tag: string | null,
): boolean {
  return mapping.tags.some(
    (entry) =>
      entry.role === role &&
      entry.tag === tag,
  );
}

function precheckLine(
  id: string,
  status: "OK" | "NG" | "未確認" | "対象外",
  detail: string,
): string {
  return `precheck ${id}: ${status} ${detail}`;
}

export function runMigrationPrecheckAtPath(
  dbPath: string,
  mapping: EndpointMapping,
  configPaths: readonly string[],
  scanProcesses: ProcessScanner =
    defaultProcessScan,
): MigrationPrecheckReport {
  const lines: string[] = [];
  let db: Database.Database | null = null;
  let databaseError: string | null = null;

  try {
    db = new Database(dbPath, {
      readonly: true,
      fileMustExist: true,
      timeout: BUSY_TIMEOUT_MS,
    });
    db.pragma(
      `busy_timeout = ${BUSY_TIMEOUT_MS}`,
    );
  } catch (error) {
    databaseError = errorMessage(error);
  }

  let schemaVersion: string | null = null;
  if (db !== null) {
    try {
      const row = db
        .prepare(
          "SELECT v FROM meta WHERE k = ?",
        )
        .get("schema_version") as
        | { v: string }
        | undefined;
      schemaVersion = row?.v ?? null;
    } catch {
      schemaVersion = null;
    }
  }
  const schemaUpgradeDetail =
    schemaVersion !== null &&
    schemaVersion !== SCHEMA_VERSION
      ? `schema_version=${schemaVersion}; run --migrate to ${SCHEMA_VERSION} first`
      : null;

  let liveDeliveries: number | null = null;
  if (
    db !== null &&
    schemaVersion === SCHEMA_VERSION
  ) {
    try {
      const now = Date.now();
      const presentedCutoff = new Date(
        now - PRESENTED_TTL_MS,
      ).toISOString();
      liveDeliveries = (
        db
          .prepare(
            `SELECT COUNT(*) AS count
               FROM deliveries
              WHERE (
                      state = 'leased'
                  AND lease_until > @now
                    )
                 OR (
                      state = 'presented'
                  AND presented_at >= @presentedCutoff
                    )`,
          )
          .get({
            now,
            presentedCutoff,
          }) as { count: number }
      ).count;
    } catch {
      liveDeliveries = null;
    }
  }

  let processScan: ProcessScanResult;
  try {
    processScan = scanProcesses();
  } catch {
    processScan = {
      available: false,
      running: 0,
      detail: "process list unavailable",
    };
  }

  /*
   * A lock row left by a migration that died after COMMIT is a database
   * every server, the hook and the sweep refuse, while every other check
   * can pass on it (Codex review of PR #42). It is check 1's business:
   * nothing may be running, and nothing can run.
   */
  const migrationLock =
    readMigrationLockAtPath(dbPath);
  if (migrationLock !== null) {
    lines.push(
      precheckLine(
        "1",
        "NG",
        `${formatMigrationLock(
          migrationLock,
        )}; restore from the backup before the precheck`,
      ),
    );
  } else if (schemaUpgradeDetail !== null) {
    lines.push(
      precheckLine(
        "1",
        "未確認",
        schemaUpgradeDetail,
      ),
    );
  } else if (
    (liveDeliveries ?? 0) > 0 ||
    (processScan.available &&
      processScan.running > 0)
  ) {
    lines.push(
      precheckLine(
        "1",
        "NG",
        `live_deliveries=${
          liveDeliveries ?? "未確認"
        } process_scan=${
          processScan.available
            ? processScan.running
            : "未確認"
        } (best-effort)`,
      ),
    );
  } else if (
    liveDeliveries === null ||
    !processScan.available
  ) {
    lines.push(
      precheckLine(
        "1",
        "未確認",
        `live_deliveries=${
          liveDeliveries ?? "未確認"
        } process_scan=${
          processScan.available
            ? processScan.running
            : "未確認"
        } (best-effort)`,
      ),
    );
  } else {
    lines.push(
      precheckLine(
        "1",
        "OK",
        `live_deliveries=0 process_scan=${processScan.running} (best-effort)`,
      ),
    );
  }

  lines.push(
    precheckLine(
      "1b",
      "対象外",
      "E-4aではbinary versionを評価しない",
    ),
  );

  const configs = readConfigs(configPaths);

  if (configPaths.length === 0) {
    lines.push(
      precheckLine(
        "2a",
        "未確認",
        "--configが指定されていない",
      ),
    );
  } else if (configs.unreadable > 0) {
    lines.push(
      precheckLine(
        "2a",
        "未確認",
        `unreadable_configs=${configs.unreadable}`,
      ),
    );
  } else {
    let retired = 0;
    for (const file of configs.files) {
      RETIRED_PATTERN.lastIndex = 0;
      retired += Array.from(
        (file.content ?? "").matchAll(
          RETIRED_PATTERN,
        ),
      ).length;
    }

    lines.push(
      precheckLine(
        "2a",
        retired === 0 ? "OK" : "NG",
        `retired_identifiers=${retired}`,
      ),
    );
  }

  if (configPaths.length === 0) {
    lines.push(
      precheckLine(
        "2b",
        "未確認",
        "--configが指定されていない",
      ),
    );
  } else if (configs.unreadable > 0) {
    lines.push(
      precheckLine(
        "2b",
        "未確認",
        `unreadable_configs=${configs.unreadable}`,
      ),
    );
  } else {
    let serverConfigs = 0;
    let hookConfigs = 0;
    let missing = 0;
    let invalid = 0;
    /*
     * The server resolves its endpoint by (role, name), so a name that
     * exists under the other role would still refuse to start after the
     * cutover. Validate the pair, not the name (Codex review of PR #42).
     */
    const endpointPairs = new Set(
      mapping.endpoints.map(
        (endpoint) =>
          `${endpoint.role}\u0000${endpoint.name}`,
      ),
    );
    const mappedDestinationPairs = new Set(
      mapping.tags.map(
        (tag) =>
          `${tag.role}\u0000${tag.endpoint}`,
      ),
    );
    const registeredServerPairs =
      new Set<string>();
    const claudeEndpointNames = new Set(
      mapping.endpoints
        .filter(
          (endpoint) =>
            endpoint.role === "claude",
        )
        .map((endpoint) => endpoint.name),
    );

    for (const file of configs.files) {
      const registrations =
        configRegistrations(file.parsed);

      for (const server of registrations.servers) {
        serverConfigs += 1;

        let startup;
        try {
          startup = parseStartupArguments(
            argumentsAfterEntry(
              server,
              SERVER_ENTRY_PATTERN,
            ),
          );
        } catch {
          invalid += 1;
          continue;
        }

        if (startup.endpointName === null) {
          missing += 1;
          continue;
        }

        const pair =
          `${startup.role}\u0000${startup.endpointName}`;
        if (!endpointPairs.has(pair)) {
          invalid += 1;
        } else {
          registeredServerPairs.add(pair);
        }
      }

      for (const hook of registrations.hooks) {
        hookConfigs += 1;

        try {
          parseEvent(
            argumentsAfterEntry(
              hook,
              HOOK_ENTRY_PATTERN,
            ),
          );
        } catch {
          invalid += 1;
          continue;
        }

        if (
          !Object.hasOwn(
            hook.env,
            "AGENT_BRIDGE_ENDPOINT",
          )
        ) {
          missing += 1;
          continue;
        }

        const endpoint =
          hook.env.AGENT_BRIDGE_ENDPOINT;
        if (
          typeof endpoint !== "string" ||
          !claudeEndpointNames.has(endpoint)
        ) {
          invalid += 1;
        }
      }
    }

    const uncovered = Array.from(
      mappedDestinationPairs,
    ).filter(
      (pair) =>
        !registeredServerPairs.has(pair),
    ).length;

    if (serverConfigs + hookConfigs === 0) {
      lines.push(
        precheckLine(
          "2b",
          "未確認",
          "registrations=0",
        ),
      );
    } else {
      lines.push(
        precheckLine(
          "2b",
          missing === 0 &&
            invalid === 0 &&
            uncovered === 0
            ? "OK"
            : "NG",
          `server_configs=${serverConfigs} hook_configs=${hookConfigs} missing=${missing} invalid=${invalid} uncovered=${uncovered}`,
        ),
      );
    }
  }

  if (db === null) {
    lines.push(
      precheckLine(
        "3",
        "未確認",
        `database=${databaseError ?? "unavailable"}`,
      ),
    );
  } else if (schemaUpgradeDetail !== null) {
    lines.push(
      precheckLine(
        "3",
        "未確認",
        schemaUpgradeDetail,
      ),
    );
  } else {
    try {
      const deliveryRows = db
        .prepare(
          `SELECT m.to_role AS role,
                  m.legacy_to_tag AS tag
             FROM deliveries d
             JOIN messages m
               ON m.message_id = d.message_id
            WHERE d.endpoint_id IS NULL`,
        )
        .all() as Array<{
        role: Role;
        tag: string | null;
      }>;
      const messageRows = db
        .prepare(
          `SELECT from_role AS role,
                  from_tag AS tag
             FROM messages
            WHERE source_endpoint_id IS NULL`,
        )
        .all() as Array<{
        role: Role;
        tag: string | null;
      }>;

      const unresolved =
        deliveryRows.filter(
          (row) =>
            !mappingResolves(
              mapping,
              row.role,
              row.tag,
            ),
        ).length +
        messageRows.filter(
          (row) =>
            !mappingResolves(
              mapping,
              row.role,
              row.tag,
            ),
        ).length;

      lines.push(
        precheckLine(
          "3",
          unresolved === 0 ? "OK" : "NG",
          `unresolved=${unresolved}`,
        ),
      );
    } catch {
      lines.push(
        precheckLine(
          "3",
          "未確認",
          "database rows could not be evaluated",
        ),
      );
    }
  }

  try {
    const directory = dirname(dbPath);
    const prefix = `${basename(
      dbPath,
    )}.pre-`;
    const candidates = readdirSync(directory)
      .filter((name) =>
        name.startsWith(prefix),
      )
      .sort()
      .reverse()
      .map((name) =>
        join(directory, name),
      );

    let databaseRoot: string | null = null;
    if (db !== null) {
      const row = db
        .prepare(
          "SELECT v FROM meta WHERE k = ?",
        )
        .get("root_id") as
        | { v: string }
        | undefined;
      databaseRoot = row?.v ?? null;
    }

    let integrityOk = 0;
    let sameRoot = 0;

    /*
     * The check answers for the newest backup, not for any backup: an
     * older intact copy of the same database must not stand in for a
     * current one that is missing or corrupt, because restoring it would
     * drop everything written since (Codex review of PR #42). Newest is
     * decided by the stamp the migration puts at the end of the name; a
     * file without one sorts last.
     */
    const stampOf = (path: string): string => {
      const named = /-(\d{8}-\d{6})$/.exec(
        basename(path),
      )?.[1];
      if (named !== undefined) {
        return `${named}-000`;
      }
      const modified = new Date(
        statSync(path).mtimeMs,
      );
      const pad = (n: number, w = 2): string =>
        String(n).padStart(w, "0");
      return `${modified.getUTCFullYear()}${pad(
        modified.getUTCMonth() + 1,
      )}${pad(modified.getUTCDate())}-${pad(
        modified.getUTCHours(),
      )}${pad(modified.getUTCMinutes())}${pad(
        modified.getUTCSeconds(),
      )}-${pad(modified.getUTCMilliseconds(), 3)}`;
    };
    const latest =
      [...candidates].sort((a, b) =>
        stampOf(b).localeCompare(stampOf(a)),
      )[0] ?? null;
    let latestOk = false;

    for (const candidate of candidates) {
      let backup: Database.Database | null =
        null;
      try {
        backup = new Database(candidate, {
          readonly: true,
          fileMustExist: true,
          timeout: BUSY_TIMEOUT_MS,
        });
        const integrity = String(
          backup.pragma("integrity_check", {
            simple: true,
          }),
        );
        if (integrity === "ok") {
          integrityOk += 1;
          const backupRoot = backup
            .prepare(
              "SELECT v FROM meta WHERE k = ?",
            )
            .get("root_id") as
            | { v: string }
            | undefined;
          if (
            databaseRoot !== null &&
            backupRoot?.v === databaseRoot
          ) {
            sameRoot += 1;
            if (candidate === latest) {
              latestOk = true;
            }
          }
        }
      } catch {
        // Counted below as an invalid candidate.
      } finally {
        backup?.close();
      }
    }

    lines.push(
      precheckLine(
        "4",
        latestOk ? "OK" : "NG",
        `backup_files=${candidates.length} integrity_ok=${integrityOk} same_root=${sameRoot} latest=${
          latest === null
            ? "none"
            : quoteForOneField(basename(latest))
        }`,
      ),
    );
  } catch {
    lines.push(
      precheckLine(
        "4",
        "未確認",
        "backup directory could not be read",
      ),
    );
  } finally {
    db?.close();
  }

  /*
   * The status sits right after the check id. A detail that happens to
   * contain ": OK " (a corrupt schema_version echoed into a 未確認 line)
   * must not read as a pass (Codex review of PR #42).
   */
  return {
    passed: lines.every((line) =>
      /^precheck [^:]+: (?:OK|対象外) /.test(
        line,
      ),
    ),
    lines,
  };
}

function parseMigrationArguments(
  argv: readonly string[],
): {
  operation: "--migrate" | "--precheck";
  mappingPath: string | null;
  configPaths: string[];
} {
  const operation = argv[0];
  if (
    operation !== "--migrate" &&
    operation !== "--precheck"
  ) {
    throw new Error("not a migration command");
  }

  let mappingPath: string | null = null;
  const configPaths: string[] = [];

  for (let index = 1; index < argv.length; index += 1) {
    const option = argv[index];
    const value = argv[index + 1];

    if (
      option === "--mapping" &&
      value &&
      mappingPath === null
    ) {
      mappingPath = value;
      index += 1;
      continue;
    }

    if (
      option === "--config" &&
      value &&
      operation === "--precheck"
    ) {
      configPaths.push(value);
      index += 1;
      continue;
    }

    throw new Error(
      operation === "--migrate"
        ? "usage: bridge-init.js --migrate [--mapping <path>]"
        : "usage: bridge-init.js --precheck --mapping <path> [--config <path>]...",
    );
  }

  if (
    operation === "--precheck" &&
    mappingPath === null
  ) {
    throw new Error(
      "usage: bridge-init.js --precheck --mapping <path> [--config <path>]...",
    );
  }

  return {
    operation,
    mappingPath,
    configPaths,
  };
}

export function runBridgeInit(
  argv = process.argv.slice(2),
): void {
  if (argv.length === 0) {
    const metadata =
      initializeFixedBridgeDatabase();
    writeErrorRecord(
      `agent-bridge initialized db=${quoteForOneField(
        metadata.dbPath,
      )} root_id=${metadata.rootId} schema_version=${metadata.schemaVersion}`,
    );
    return;
  }

  if (
    argv[0] === "--migrate" ||
    argv[0] === "--precheck"
  ) {
    const parsed =
      parseMigrationArguments(argv);
    const mapping =
      parsed.mappingPath === null
        ? undefined
        : loadMapping(parsed.mappingPath);

    if (parsed.operation === "--migrate") {
      const metadata =
        migrateFixedBridgeDatabase({
          mapping,
          pauseAfterDestructiveDdl:
            process.env[
              MIGRATION_PAUSE_ENV
            ] === "1",
        });
      writeErrorRecord(
        `agent-bridge migrated db=${quoteForOneField(
          metadata.dbPath,
        )} root_id=${metadata.rootId} schema_version=${metadata.schemaVersion} backup=${quoteForOneField(
          metadata.backupPath,
        )}`,
      );
      return;
    }

    const report =
      runMigrationPrecheckAtPath(
        getBridgeDbPath(),
        mapping!,
        parsed.configPaths,
      );

    for (const line of report.lines) {
      writeErrorRecord(
        `agent-bridge ${line}`,
      );
    }

    if (!report.passed) {
      process.exitCode = 1;
    }
    return;
  }

  if (
    argv.length === 3 &&
    argv[0] === "--add-endpoint"
  ) {
    const role = argv[1];

    if (
      role !== "claude" &&
      role !== "codex"
    ) {
      throw new Error(
        "usage: bridge-init.js --add-endpoint claude|codex <name>",
      );
    }

    const bus = BridgeBus.open();

    try {
      const endpoint = bus.addEndpoint(
        role,
        argv[2] ?? "",
      );

      writeErrorRecord(
        `agent-bridge endpoint added role=${endpoint.role} name=${quoteForOneField(
          endpoint.name,
        )} endpoint_id=${endpoint.endpoint_id} db=${quoteForOneField(
          bus.dbPath,
        )}`,
      );
    } finally {
      bus.close();
    }

    return;
  }

  if (
    argv.length === 2 &&
    (argv[0] === "--require-tag" ||
      argv[0] === "--strict-addressing")
  ) {
    const key =
      argv[0] === "--require-tag"
        ? "require_tag"
        : "strict_addressing";
    const value = argv[1] ?? "";
    const bus = BridgeBus.open();

    try {
      bus.setRolePolicy(key, value);
      const roles = [
        ...bus.policyRoles(key),
      ].sort();

      writeErrorRecord(
        `agent-bridge ${key}=${
          roles.length === 0
            ? "none"
            : roles.join(",")
        } db=${quoteForOneField(bus.dbPath)}`,
      );
    } finally {
      bus.close();
    }

    return;
  }

  throw new Error(
    "usage: bridge-init.js [--migrate [--mapping <path>] | --precheck --mapping <path> [--config <path>]... | --add-endpoint claude|codex <name> | --require-tag <roles> | --strict-addressing <roles>]",
  );
}

if (isDirectExecution()) {
  try {
    runBridgeInit();
  } catch (error) {
    writeErrorRecord(
      `agent-bridge init failed: ${errorMessage(error)}`,
    );
    process.exitCode = 1;
  }
}