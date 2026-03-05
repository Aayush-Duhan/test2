import { existsSync, mkdirSync, readFileSync } from "fs";
import path from "path";
import { DatabaseSync } from "node:sqlite";

let singleton: DatabaseSync | null = null;

function resolveProjectRoot() {
  const candidates = [process.cwd(), path.resolve(process.cwd(), "..")];
  for (const root of candidates) {
    if (existsSync(path.join(root, "db", "schema_v1.sql"))) {
      return root;
    }
  }
  return process.cwd();
}

export function resolveSqlitePath() {
  const configured = process.env.APP_SQLITE_PATH?.trim();
  if (configured) {
    return path.resolve(configured);
  }
  const root = resolveProjectRoot();
  return path.join(root, "data", "app.db");
}

function resolveSchemaPath() {
  const root = resolveProjectRoot();
  return path.join(root, "db", "schema_v1.sql");
}

function applyPragmas(db: DatabaseSync) {
  db.exec("PRAGMA journal_mode=WAL");
  db.exec("PRAGMA busy_timeout=5000");
  db.exec("PRAGMA foreign_keys=ON");
}

function ensureSchema(db: DatabaseSync) {
  const schemaPath = resolveSchemaPath();
  const sql = readFileSync(schemaPath, "utf-8");
  db.exec(sql);
  const columns = db
    .prepare("PRAGMA table_info(runs)")
    .all() as Array<{ name: string }>;
  if (!columns.some((column) => column.name === "missing_objects_json")) {
    db.exec("ALTER TABLE runs ADD COLUMN missing_objects_json TEXT NOT NULL DEFAULT '[]'");
  }
  db.prepare(
    "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES (?, ?)"
  ).run("v1", new Date().toISOString());
}

export function getSqliteDb() {
  if (singleton) return singleton;
  const dbPath = resolveSqlitePath();
  mkdirSync(path.dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  applyPragmas(db);
  ensureSchema(db);
  singleton = db;
  return singleton;
}
