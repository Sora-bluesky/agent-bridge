import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  BridgeBus,
  initializeFixedBridgeDatabase,
  migrateFixedBridgeDatabase,
} from "./db.js";

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

function oneLineError(
  error: unknown,
): string {
  return (
    error instanceof Error
      ? error.message
      : String(error)
  ).replace(/[\r\n]+/g, " ");
}

export function runBridgeInit(
  argv = process.argv.slice(2),
): void {
  if (argv.length === 0) {
    const metadata =
      initializeFixedBridgeDatabase();
    console.error(
      `agent-bridge initialized db=${JSON.stringify(
        metadata.dbPath,
      )} root_id=${metadata.rootId} schema_version=${metadata.schemaVersion}`,
    );
    return;
  }

  if (
    argv.length === 1 &&
    argv[0] === "--migrate"
  ) {
    const metadata =
      migrateFixedBridgeDatabase();
    console.error(
      `agent-bridge migrated db=${JSON.stringify(
        metadata.dbPath,
      )} root_id=${metadata.rootId} schema_version=${metadata.schemaVersion}`,
    );
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

      console.error(
        `agent-bridge endpoint added role=${endpoint.role} name=${endpoint.name} endpoint_id=${endpoint.endpoint_id} db=${JSON.stringify(
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

      console.error(
        `agent-bridge ${key}=${
          roles.length === 0
            ? "none"
            : roles.join(",")
        } db=${JSON.stringify(bus.dbPath)}`,
      );
    } finally {
      bus.close();
    }

    return;
  }

  throw new Error(
    "usage: bridge-init.js [--migrate | --add-endpoint claude|codex <name> | --require-tag <roles> | --strict-addressing <roles>]",
  );
}

if (isDirectExecution()) {
  try {
    runBridgeInit();
  } catch (error) {
    console.error(
      `agent-bridge init failed: ${oneLineError(error)}`,
    );
    process.exitCode = 1;
  }
}
