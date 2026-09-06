import { execFileSync } from "node:child_process";
import {
  readFileSync,
  readdirSync,
} from "node:fs";
import {
  basename,
  dirname,
  join,
  resolve,
} from "node:path";
import { pathToFileURL } from "node:url";
import Database from "better-sqlite3";
import {
  BUSY_TIMEOUT_MS,
  BridgeBus,
  type EndpointMapping,
  getBridgeDbPath,
  initializeFixedBridgeDatabase,
  MIGRATION_PAUSE_ENV,
  migrateFixedBridgeDatabase,
  PRESENTED_TTL_MS,
  type Role,
  validateEndpointMapping,
} from "./db.js";
import {
  errorMessage,
  quoteForOneField,
  writeErrorRecord,
} from "./one-line.js";

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
const ENDPOINT_ARGUMENT_PATTERN =
  /--endpoint(?:(?:["']?\s*,\s*["']?)|(?:\s*=\s*["']?)|(?:\s+["']?))([^"',\]}\s]+)/g;
const AGENT_BRIDGE_SERVER_OPTION_PATTERN =
  /(?:^|[\s"'])--(?:role["']?(?:\s+|=)["']?(?:claude|codex)(?:["'\s]|$)|endpoint(?:["'\s=]|$))/i;

interface ConfigRead {
  path: string;
  content: string | null;
}

interface ConfigRegistrations {
  servers: string[];
  hooks: string[];
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

function commandAndArgs(
  entry: Record<string, unknown>,
): string {
  const fields: string[] = [];

  if (typeof entry.command === "string") {
    fields.push(entry.command);
  }

  if (Array.isArray(entry.args)) {
    fields.push(
      ...entry.args.filter(
        (argument): argument is string =>
          typeof argument === "string",
      ),
    );
  }

  return fields.join(" ");
}

function jsonConfigRegistrations(
  content: string,
): ConfigRegistrations | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(content);
  } catch {
    return null;
  }

  const registrations: ConfigRegistrations = {
    servers: [],
    hooks: [],
  };
  if (!isRecord(parsed)) {
    return registrations;
  }

  /*
   * Registrations live at any depth: ~/.claude.json keeps a top-level
   * mcpServers and one more under every projects[<path>], and hooks sit
   * under settings and project blocks alike. Walk the whole tree, and
   * classify by where an object sits, not by what its command line
   * happens to mention: a value of an `mcpServers` record is a server
   * candidate, anything under a `hooks` key is a hook candidate. A hook
   * whose command mentions server.js in passing stays a hook.
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

    const command = commandAndArgs(value);
    if (
      context === "servers" &&
      patternMatches(
        SERVER_ENTRY_PATTERN,
        command,
      )
    ) {
      registrations.servers.push(command);
      return;
    }
    if (
      context === "hooks" &&
      patternMatches(
        HOOK_ENTRY_PATTERN,
        command,
      )
    ) {
      const environment =
        JSON.stringify(value.env) ?? "";
      registrations.hooks.push(
        `${command}\n${environment}`,
      );
      return;
    }

    /*
     * The container keys only mean something outside a container: a
     * server registered under the name "hooks" is still a server, and a
     * registration never nests the other kind.
     */
    for (const [key, nested] of Object.entries(
      value,
    )) {
      visit(
        nested,
        context !== null
          ? context
          : key === "mcpServers"
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

function tomlConfigRegistrations(
  content: string,
): ConfigRegistrations {
  const registrations: ConfigRegistrations = {
    servers: [],
    hooks: [],
  };
  const headerPattern =
    /^\s*(\[\[?)([^\]\r\n]+)\]\]?\s*(?:#.*)?$/gm;
  const headers = Array.from(
    content.matchAll(headerPattern),
  );

  for (
    let index = 0;
    index < headers.length;
    index += 1
  ) {
    const header = headers[index]!;
    const next = headers[index + 1];
    const opening = header[1] ?? "";
    const name = (header[2] ?? "").trim();
    const start = header.index ?? 0;
    const end = next?.index ?? content.length;
    const block = content.slice(start, end);

    if (
      opening === "[" &&
      /^mcp_servers\.(?:"[^"]+"|'[^']+'|[^.]+)$/i.test(
        name,
      ) &&
      patternMatches(
        SERVER_ENTRY_PATTERN,
        block,
      )
    ) {
      registrations.servers.push(block);
    } else if (
      opening === "[[" &&
      /^hooks(?:\.|$)/i.test(name) &&
      patternMatches(
        HOOK_ENTRY_PATTERN,
        block,
      )
    ) {
      registrations.hooks.push(block);
    }
  }

  return registrations;
}

function configRegistrations(
  content: string,
): ConfigRegistrations {
  return (
    jsonConfigRegistrations(content) ??
    tomlConfigRegistrations(content)
  );
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
      return {
        path: absolute,
        content: readFileSync(
          absolute,
          "utf8",
        ),
      };
    } catch {
      unreadable += 1;
      return {
        path: absolute,
        content: null,
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
          "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress",
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
  } catch {
    return {
      available: false,
      running: 0,
      detail: "process list unavailable",
    };
  }
}

function endpointArguments(
  content: string,
): string[] {
  ENDPOINT_ARGUMENT_PATTERN.lastIndex = 0;
  return Array.from(
    content.matchAll(
      ENDPOINT_ARGUMENT_PATTERN,
    ),
    (match) => match[1] ?? "",
  );
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

  let liveDeliveries: number | null = null;
  if (db !== null) {
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

  if (
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
    const endpointNames = new Set(
      mapping.endpoints.map(
        (endpoint) => endpoint.name,
      ),
    );

    for (const file of configs.files) {
      const registrations =
        configRegistrations(
          file.content ?? "",
        );

      for (const server of registrations.servers) {
        serverConfigs += 1;
        const names =
          endpointArguments(server);
        if (names.length === 0) {
          missing += 1;
        } else if (
          names.some(
            (name) =>
              !endpointNames.has(name),
          )
        ) {
          invalid += 1;
        }
      }

      for (const hook of registrations.hooks) {
        hookConfigs += 1;
        if (
          !/\bAGENT_BRIDGE_ENDPOINT\b/.test(
            hook,
          )
        ) {
          missing += 1;
        }
      }
    }

    lines.push(
      precheckLine(
        "2b",
        missing === 0 && invalid === 0
          ? "OK"
          : "NG",
        `server_configs=${serverConfigs} hook_configs=${hookConfigs} missing=${missing} invalid=${invalid}`,
      ),
    );
  }

  if (db === null) {
    lines.push(
      precheckLine(
        "3",
        "未確認",
        `database=${databaseError ?? "unavailable"}`,
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
        sameRoot > 0 ? "OK" : "NG",
        `backup_files=${candidates.length} integrity_ok=${integrityOk} same_root=${sameRoot}`,
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

  return {
    passed: lines.every(
      (line) =>
        line.startsWith(
          "precheck 1b: 対象外",
        ) ||
        line.includes(": OK "),
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