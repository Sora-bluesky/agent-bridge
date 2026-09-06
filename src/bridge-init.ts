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
  /^--role (?:claude|codex) --endpoint (\S(?:.*\S)?)$/g;
const AGENT_BRIDGE_SERVER_OPTION_PATTERN =
  /(?:^|[\s"'])--(?:role["']?(?:\s+|=)["']?(?:claude|codex)(?:["'\s]|$)|endpoint(?:["'\s=]|$))/i;

interface ConfigRead {
  path: string;
  content: string | null;
}

interface ConfigRegistration {
  command: string;
  args: string[] | null;
  env: Record<string, unknown> | null;
  text: string;
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

function jsonRegistration(
  entry: Record<string, unknown>,
): ConfigRegistration {
  const command =
    typeof entry.command === "string"
      ? entry.command
      : "";
  const args = Array.isArray(entry.args)
    ? entry.args.filter(
        (argument): argument is string =>
          typeof argument === "string",
      )
    : null;

  return {
    command,
    args,
    env: isRecord(entry.env)
      ? entry.env
      : null,
    text: commandAndArgs(entry),
  };
}

function tomlQuotedAssignment(
  content: string,
  name: string,
): string | null {
  const assignment = new RegExp(
    `^\\s*${name}\\s*=\\s*(["'])`,
    "m",
  ).exec(content);
  if (assignment === null) {
    return null;
  }

  const quote = assignment[1] ?? "";
  const start =
    (assignment.index ?? 0) +
    assignment[0].length;
  let value = "";

  for (
    let index = start;
    index < content.length;
    index += 1
  ) {
    const character = content[index] ?? "";
    if (
      quote === '"' &&
      character === "\\" &&
      content[index + 1] === '"'
    ) {
      value += '"';
      index += 1;
    } else if (character === quote) {
      return value;
    } else {
      value += character;
    }
  }

  return null;
}

function tomlArguments(
  content: string,
): string[] | null {
  const assignment =
    /^\s*args\s*=\s*\[/m.exec(content);
  if (assignment === null) {
    return null;
  }

  const args: string[] = [];
  let quote: '"' | "'" | null = null;
  let value = "";
  const start =
    (assignment.index ?? 0) +
    assignment[0].length;

  for (
    let index = start;
    index < content.length;
    index += 1
  ) {
    const character = content[index] ?? "";
    if (quote === null) {
      if (character === "]") {
        return args;
      }
      if (
        character === '"' ||
        character === "'"
      ) {
        quote = character;
        value = "";
      }
      continue;
    }

    if (
      quote === '"' &&
      character === "\\" &&
      content[index + 1] === '"'
    ) {
      value += '"';
      index += 1;
    } else if (character === quote) {
      args.push(value);
      quote = null;
    } else {
      value += character;
    }
  }

  return args;
}

function tomlEnvironment(
  content: string,
): Record<string, unknown> | null {
  const envHeader =
    /^\s*\[hooks\.env\]\s*(?:#.*)?$/m.exec(
      content,
    );
  if (envHeader === null) {
    return null;
  }

  const sectionStart =
    (envHeader.index ?? 0) +
    envHeader[0].length;
  const remainder =
    content.slice(sectionStart);
  const nextHeader =
    /^\s*\[\[?[^\]\r\n]+\]\]?\s*(?:#.*)?$/m.exec(
      remainder,
    );
  const section = remainder.slice(
    0,
    nextHeader?.index ?? remainder.length,
  );
  const endpoint = tomlQuotedAssignment(
    section,
    "AGENT_BRIDGE_ENDPOINT",
  );
  const environment: Record<string, unknown> =
    {};
  if (endpoint !== null) {
    environment.AGENT_BRIDGE_ENDPOINT =
      endpoint;
  }
  return environment;
}

function tomlRegistration(
  content: string,
): ConfigRegistration {
  const command =
    tomlQuotedAssignment(content, "command") ??
    "";
  const args = tomlArguments(content);

  return {
    command,
    args,
    env: tomlEnvironment(content),
    text: [command, ...(args ?? [])]
      .filter((field) => field.length > 0)
      .join(" "),
  };
}

function tomlProblem(
  content: string,
): string | null {
  const keysByTable = new Map<
    string,
    Set<string>
  >();
  const arrayCounts = new Map<
    string,
    number
  >();
  const activeArrayTables = new Map<
    string,
    string
  >();
  let currentTable = "<root>";
  let arrayDepth = 0;

  const scanFragment = (
    fragment: string,
    startingDepth: number,
  ): {
    depth: number;
    problem: string | null;
  } => {
    let depth = startingDepth;
    let quote: '"' | "'" | null = null;

    for (
      let index = 0;
      index < fragment.length;
      index += 1
    ) {
      const character =
        fragment[index] ?? "";
      if (quote !== null) {
        if (
          quote === '"' &&
          character === "\\"
        ) {
          index += 1;
        } else if (character === quote) {
          quote = null;
        }
        continue;
      }

      if (character === "#") {
        break;
      }
      if (
        character === '"' ||
        character === "'"
      ) {
        quote = character;
      } else if (character === "[") {
        depth += 1;
      } else if (character === "]") {
        depth -= 1;
        if (depth < 0) {
          return {
            depth,
            problem:
              "unexpected array close",
          };
        }
      }
    }

    return {
      depth,
      problem:
        quote === null
          ? null
          : "unterminated string",
    };
  };

  const contextualTable = (
    name: string,
  ): string => {
    let ancestor: string | null = null;
    for (
      const candidate of
        activeArrayTables.keys()
    ) {
      if (
        (name === candidate ||
          name.startsWith(
            `${candidate}.`,
          )) &&
        (ancestor === null ||
          candidate.length >
            ancestor.length)
      ) {
        ancestor = candidate;
      }
    }

    if (ancestor === null) {
      return name;
    }
    return `${
      activeArrayTables.get(ancestor) ??
      ancestor
    }${name.slice(ancestor.length)}`;
  };

  const lines = content.split(/\r?\n/);
  for (
    let lineIndex = 0;
    lineIndex < lines.length;
    lineIndex += 1
  ) {
    const line = lines[lineIndex] ?? "";
    const lineNumber = lineIndex + 1;
    const header =
      /^\s*(\[\[?)([^\]\r\n]+)(\]\]?)\s*(?:#.*)?$/.exec(
        line,
      );

    if (header !== null) {
      if (arrayDepth > 0) {
        return `line ${lineNumber}: unterminated array before header`;
      }

      const opening = header[1] ?? "";
      const closing = header[3] ?? "";
      const isArrayHeader =
        opening === "[[";
      if (
        (isArrayHeader &&
          closing !== "]]") ||
        (!isArrayHeader &&
          closing !== "]")
      ) {
        return `line ${lineNumber}: malformed table header`;
      }

      const rawName =
        (header[2] ?? "").trim();
      const headerScan = scanFragment(
        rawName,
        0,
      );
      if (
        rawName.length === 0 ||
        headerScan.problem !== null ||
        headerScan.depth !== 0
      ) {
        return `line ${lineNumber}: malformed table header`;
      }

      const name = rawName.replace(
        /\s*\.\s*/g,
        ".",
      );
      if (isArrayHeader) {
        for (
          const activeName of Array.from(
            activeArrayTables.keys(),
          )
        ) {
          if (
            activeName === name ||
            activeName.startsWith(
              `${name}.`,
            )
          ) {
            activeArrayTables.delete(
              activeName,
            );
          }
        }

        const instance =
          (arrayCounts.get(name) ?? 0) +
          1;
        arrayCounts.set(name, instance);
        currentTable =
          `${contextualTable(name)}#${instance}`;
        activeArrayTables.set(
          name,
          currentTable,
        );
      } else {
        currentTable =
          contextualTable(name);
      }
      continue;
    }

    if (
      /^\s*$/.test(line) ||
      /^\s*#/.test(line)
    ) {
      continue;
    }

    if (arrayDepth > 0) {
      const continuation = scanFragment(
        line,
        arrayDepth,
      );
      if (continuation.problem !== null) {
        return `line ${lineNumber}: ${continuation.problem}`;
      }
      arrayDepth = continuation.depth;
      continue;
    }

    const assignment =
      /^\s*([A-Za-z0-9_-]+(?:\s*\.\s*[A-Za-z0-9_-]+)*)\s*=\s*(.*)$/.exec(
        line,
      );
    if (assignment === null) {
      return `line ${lineNumber}: unrecognized TOML line`;
    }

    const key = (assignment[1] ?? "")
      .split(/\s*\.\s*/)
      .join(".");
    const value = assignment[2] ?? "";
    if (value.trim().length === 0) {
      return `line ${lineNumber}: missing value`;
    }

    let tableKeys =
      keysByTable.get(currentTable);
    if (tableKeys === undefined) {
      tableKeys = new Set<string>();
      keysByTable.set(
        currentTable,
        tableKeys,
      );
    }
    if (tableKeys.has(key)) {
      return `line ${lineNumber}: duplicate key ${key}`;
    }
    tableKeys.add(key);

    const valueScan = scanFragment(
      value,
      0,
    );
    if (valueScan.problem !== null) {
      return `line ${lineNumber}: ${valueScan.problem}`;
    }
    arrayDepth = valueScan.depth;
  }

  return arrayDepth === 0
    ? null
    : "unterminated array at end of file";
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

    const registration =
      jsonRegistration(value);
    if (
      context === "servers" &&
      patternMatches(
        SERVER_ENTRY_PATTERN,
        registration.text,
      )
    ) {
      registrations.servers.push(
        registration,
      );
      return;
    }
    if (
      context === "hooks" &&
      patternMatches(
        HOOK_ENTRY_PATTERN,
        registration.text,
      )
    ) {
      registrations.hooks.push(
        registration,
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
    const opening = header[1] ?? "";
    const name = (header[2] ?? "").trim();
    const start = header.index ?? 0;
    let nextIndex = index + 1;
    while (nextIndex < headers.length) {
      const nestedName =
        (headers[nextIndex]![2] ?? "").trim();
      if (
        !nestedName.startsWith(`${name}.`)
      ) {
        break;
      }
      nextIndex += 1;
    }
    const end =
      headers[nextIndex]?.index ??
      content.length;
    const block = content.slice(start, end);
    const registration =
      tomlRegistration(block);

    if (
      opening === "[" &&
      /^mcp_servers\.(?:"[^"]+"|'[^']+'|[^.]+)$/i.test(
        name,
      ) &&
      patternMatches(
        SERVER_ENTRY_PATTERN,
        registration.text,
      )
    ) {
      registrations.servers.push(
        registration,
      );
    } else if (
      opening === "[[" &&
      /^hooks(?:\.|$)/i.test(name) &&
      patternMatches(
        HOOK_ENTRY_PATTERN,
        registration.text,
      )
    ) {
      registrations.hooks.push(
        registration,
      );
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
      const content = readFileSync(
        absolute,
        "utf8",
      );
      /*
       * A file that looks like JSON but does not parse is not a config
       * the application could load either. Falling back to the TOML
       * scanner would read zero registrations out of it and let checks
       * 2a and 2b pass on nothing (Codex review of PR #42).
       */
      /*
       * Only an object counts as JSON here: a TOML file opens with a
       * table header, "[mcp_servers.codex]", which is not JSON and must
       * not be refused as broken JSON.
       */
      if (/^\s*\{/.test(content)) {
        JSON.parse(content);
      } else {
        const looksLikeToml =
          /^\s*(?:\[\[?[^\]\r\n]+\]\]?|[A-Za-z0-9_-]+(?:\s*\.\s*[A-Za-z0-9_-]+)*\s*=)/m.test(
            content,
          );
        if (
          looksLikeToml &&
          tomlProblem(content) !== null
        ) {
          throw new Error(
            "TOML config is malformed",
          );
        }
      }
      return { path: absolute, content };
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

const ROLE_ARGUMENT_PATTERN =
  /^--role (claude|codex)(?: --endpoint \S(?:.*\S)?)?$/;

function roleArgument(
  content: string,
): Role | null {
  const match = ROLE_ARGUMENT_PATTERN.exec(
    content,
  );
  return match === null
    ? null
    : (match[1] as Role);
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

function argumentValues(
  args: readonly string[],
  option: string,
): string[] {
  const values: string[] = [];

  for (
    let index = 0;
    index < args.length;
    index += 1
  ) {
    const argument = args[index] ?? "";
    if (argument === option) {
      if (index + 1 < args.length) {
        values.push(args[index + 1] ?? "");
        index += 1;
      }
    } else if (
      argument.startsWith(`${option}=`)
    ) {
      values.push(
        argument.slice(option.length + 1),
      );
    }
  }

  return values;
}

type ServerRegistrationArguments =
  | { status: "missing" }
  | { status: "invalid" }
  | {
      status: "ready";
      role: Role;
      endpoint: string;
    };

function registrationServerArguments(
  registration: ConfigRegistration,
): ServerRegistrationArguments {
  if (registration.args === null) {
    SERVER_ENTRY_PATTERN.lastIndex = 0;
    const script =
      SERVER_ENTRY_PATTERN.exec(
        registration.text,
      );
    SERVER_ENTRY_PATTERN.lastIndex = 0;
    if (script === null) {
      return { status: "invalid" };
    }

    const remainder = registration.text
      .slice(
        (script.index ?? 0) +
          script[0].length,
      )
      .trim();
    const role = roleArgument(remainder);
    if (role === null) {
      return { status: "invalid" };
    }

    const endpoint =
      endpointArguments(remainder)[0];
    return endpoint === undefined
      ? { status: "missing" }
      : {
          status: "ready",
          role,
          endpoint,
        };
  }

  const invocation = [
    ...commandTokens(registration.command),
    ...registration.args,
  ];
  const scriptIndex =
    invocation.findIndex((argument) =>
      patternMatches(
        SERVER_ENTRY_PATTERN,
        argument,
      ),
    );
  if (scriptIndex < 0) {
    return { status: "invalid" };
  }

  const remainder = invocation.slice(
    scriptIndex + 1,
  );
  const role = argumentValues(
    remainder,
    "--role",
  )[0];
  if (
    remainder.length === 2 &&
    remainder[0] === "--role" &&
    isRole(role)
  ) {
    return { status: "missing" };
  }

  if (
    remainder.length !== 4 ||
    remainder[0] !== "--role" ||
    remainder[2] !== "--endpoint"
  ) {
    return { status: "invalid" };
  }

  const endpoint = argumentValues(
    remainder,
    "--endpoint",
  )[0];
  if (
    !isRole(role) ||
    endpoint === undefined ||
    endpoint.length === 0
  ) {
    return { status: "invalid" };
  }

  return {
    status: "ready",
    role,
    endpoint,
  };
}

function isRole(
  value: string | undefined,
): value is Role {
  return (
    value === "claude" ||
    value === "codex"
  );
}

function commandTokens(
  command: string,
): string[] {
  const tokens: string[] = [];
  let token = "";
  let tokenStarted = false;
  let quote: '"' | "'" | null = null;

  for (
    let index = 0;
    index < command.length;
    index += 1
  ) {
    const character = command[index] ?? "";
    if (quote !== null) {
      if (
        quote === '"' &&
        character === "\\" &&
        command[index + 1] === '"'
      ) {
        token += '"';
        index += 1;
      } else if (character === quote) {
        quote = null;
      } else {
        token += character;
      }
      continue;
    }

    if (/\s/.test(character)) {
      if (tokenStarted) {
        tokens.push(token);
        token = "";
        tokenStarted = false;
      }
    } else if (
      character === '"' ||
      character === "'"
    ) {
      quote = character;
      tokenStarted = true;
    } else {
      token += character;
      tokenStarted = true;
    }
  }

  if (tokenStarted) {
    tokens.push(token);
  }
  return tokens;
}

function commandEndpointAssignment(
  command: string,
): string | null {
  const prefix = "AGENT_BRIDGE_ENDPOINT=";
  const assignment = commandTokens(
    command,
  ).find((token) =>
    token.startsWith(prefix),
  );
  return assignment === undefined
    ? null
    : assignment.slice(prefix.length);
}

function hookEndpoint(
  registration: ConfigRegistration,
): string | null {
  const configured =
    registration.env
      ?.AGENT_BRIDGE_ENDPOINT;
  if (typeof configured === "string") {
    return configured;
  }
  return commandEndpointAssignment(
    registration.command,
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
        configRegistrations(
          file.content ?? "",
        );

      for (const server of registrations.servers) {
        serverConfigs += 1;
        const serverArguments =
          registrationServerArguments(
            server,
          );
        if (
          serverArguments.status ===
          "missing"
        ) {
          missing += 1;
        } else if (
          serverArguments.status ===
          "invalid"
        ) {
          invalid += 1;
        } else if (
          !endpointPairs.has(
            `${serverArguments.role}\u0000${serverArguments.endpoint}`,
          )
        ) {
          invalid += 1;
        }
      }

      for (const hook of registrations.hooks) {
        hookConfigs += 1;
        const endpoint = hookEndpoint(hook);
        if (endpoint === null) {
          missing += 1;
        } else if (
          !claudeEndpointNames.has(endpoint)
        ) {
          invalid += 1;
        }
      }
    }

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
          missing === 0 && invalid === 0
            ? "OK"
            : "NG",
          `server_configs=${serverConfigs} hook_configs=${hookConfigs} missing=${missing} invalid=${invalid}`,
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