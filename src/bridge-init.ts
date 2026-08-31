import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import {
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

  throw new Error(
    "usage: bridge-init.js [--migrate]",
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
