import { promises as fs } from "fs";
import path from "path";
import { getSqliteDb } from "./sqlite";

export interface ProjectRecord {
  projectId: string;
  name: string;
  sourceLanguage?: string;
  createdAt: string;
}

export interface SourceRecord {
  sourceId: string;
  projectId: string;
  filename: string;
  filepath: string;
  createdAt: string;
}

export interface SchemaRecord {
  schemaId: string;
  projectId: string;
  filename: string;
  filepath: string;
  createdAt: string;
}

const dataDir = path.join(process.cwd(), "data");
const uploadsDir = path.join(process.cwd(), "uploads");
const outputsDir = path.join(process.cwd(), "outputs");

async function ensureDir(target: string) {
  await fs.mkdir(target, { recursive: true });
}

export async function ensureStorage() {
  await ensureDir(dataDir);
  await ensureDir(uploadsDir);
  await ensureDir(outputsDir);
  getSqliteDb();
}

export async function saveProject(project: ProjectRecord) {
  const db = getSqliteDb();
  db.prepare(
    `
      INSERT INTO projects(project_id, name, source_language, created_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(project_id) DO UPDATE SET
        name = excluded.name,
        source_language = excluded.source_language,
        created_at = excluded.created_at
    `
  ).run(
    project.projectId,
    project.name,
    project.sourceLanguage ?? null,
    project.createdAt
  );
}

export async function getProject(projectId: string): Promise<ProjectRecord | undefined> {
  const db = getSqliteDb();
  const row = db
    .prepare(
      `
        SELECT project_id, name, source_language, created_at
        FROM projects
        WHERE project_id = ?
      `
    )
    .get(projectId) as
    | { project_id: string; name: string; source_language: string | null; created_at: string }
    | undefined;
  if (!row) return undefined;
  return {
    projectId: row.project_id,
    name: row.name,
    sourceLanguage: row.source_language ?? undefined,
    createdAt: row.created_at,
  };
}

export async function saveSource(source: SourceRecord) {
  const db = getSqliteDb();
  db.prepare(
    `
      INSERT INTO sources(source_id, project_id, filename, filepath, created_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(source_id) DO UPDATE SET
        project_id = excluded.project_id,
        filename = excluded.filename,
        filepath = excluded.filepath,
        created_at = excluded.created_at
    `
  ).run(source.sourceId, source.projectId, source.filename, source.filepath, source.createdAt);
}

export async function getSource(sourceId: string): Promise<SourceRecord | undefined> {
  const db = getSqliteDb();
  const row = db
    .prepare(
      `
        SELECT source_id, project_id, filename, filepath, created_at
        FROM sources
        WHERE source_id = ?
      `
    )
    .get(sourceId) as
    | {
        source_id: string;
        project_id: string;
        filename: string;
        filepath: string;
        created_at: string;
      }
    | undefined;
  if (!row) return undefined;
  return {
    sourceId: row.source_id,
    projectId: row.project_id,
    filename: row.filename,
    filepath: row.filepath,
    createdAt: row.created_at,
  };
}

export async function saveSchema(schema: SchemaRecord) {
  const db = getSqliteDb();
  db.prepare(
    `
      INSERT INTO schemas(schema_id, project_id, filename, filepath, created_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(schema_id) DO UPDATE SET
        project_id = excluded.project_id,
        filename = excluded.filename,
        filepath = excluded.filepath,
        created_at = excluded.created_at
    `
  ).run(schema.schemaId, schema.projectId, schema.filename, schema.filepath, schema.createdAt);
}

export async function getSchema(schemaId: string): Promise<SchemaRecord | undefined> {
  const db = getSqliteDb();
  const row = db
    .prepare(
      `
        SELECT schema_id, project_id, filename, filepath, created_at
        FROM schemas
        WHERE schema_id = ?
      `
    )
    .get(schemaId) as
    | {
        schema_id: string;
        project_id: string;
        filename: string;
        filepath: string;
        created_at: string;
      }
    | undefined;
  if (!row) return undefined;
  return {
    schemaId: row.schema_id,
    projectId: row.project_id,
    filename: row.filename,
    filepath: row.filepath,
    createdAt: row.created_at,
  };
}

export async function getUploadDir(projectId: string) {
  const dir = path.join(uploadsDir, projectId);
  await ensureDir(dir);
  return dir;
}

export async function getOutputDir(projectId: string, runId: string) {
  const dir = path.join(outputsDir, projectId, runId);
  await ensureDir(dir);
  return dir;
}

export { uploadsDir, outputsDir };
