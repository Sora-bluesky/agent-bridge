import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
  BridgeBus,
  initializeFixedBridgeDatabase,
  migrateFixedBridgeDatabase,
} from "./db.js";
import {
  errorMessage,
  quoteForOneField,
  writeErrorRecord,
} from "./one-line.js";

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
    argv.length === 1 &&
    argv[0] === "--migrate"
  ) {
    const metadata =
      migrateFixedBridgeDatabase();
    writeErrorRecord(
      `agent-bridge migrated db=${quoteForOneField(
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

      /*
       * The name is the text the operator typed, and a space or an
       * equals sign in it would otherwise read as another field of this
       * record. The role beside it is one of two words this code
       * checked, and the identifier is a `randomUUID` it wrote.
       */
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
    "usage: bridge-init.js [--migrate | --add-endpoint claude|codex <name> | --require-tag <roles> | --strict-addressing <roles>]",
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
