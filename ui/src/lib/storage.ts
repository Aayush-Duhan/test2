import { promises as fs } from "fs";
import path from "path";

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
const projectsPath = path.join(dataDir, "projects.json");
const sourcesPath = path.join(dataDir, "sources.json");
const schemasPath = path.join(dataDir, "schemas.json");
const runsPath = path.join(dataDir, "runs.json");

async function ensureDir(target: string) {
  await fs.mkdir(target, { recursive: true });
}

export async function ensureStorage() {
  await ensureDir(dataDir);
  await ensureDir(uploadsDir);
  await ensureDir(outputsDir);
}

async function readJson<T>(file: string, fallback: T): Promise<T> {
  try {
    const raw = await fs.readFile(file, "utf-8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

async function writeJson<T>(file: string, payload: T) {
  await ensureDir(path.dirname(file));
  await fs.writeFile(file, JSON.stringify(payload, null, 2), "utf-8");
}

export async function saveProject(project: ProjectRecord) {
  const data = await readJson<ProjectRecord[]>(projectsPath, []);
  const next = data.filter((item) => item.projectId !== project.projectId);
  next.push(project);
  await writeJson(projectsPath, next);
}

export async function getProject(projectId: string): Promise<ProjectRecord | undefined> {
  const data = await readJson<ProjectRecord[]>(projectsPath, []);
  return data.find((item) => item.projectId === projectId);
}

export async function saveSource(source: SourceRecord) {
  const data = await readJson<SourceRecord[]>(sourcesPath, []);
  const next = data.filter((item) => item.sourceId !== source.sourceId);
  next.push(source);
  await writeJson(sourcesPath, next);
}

export async function getSource(sourceId: string): Promise<SourceRecord | undefined> {
  const data = await readJson<SourceRecord[]>(sourcesPath, []);
  return data.find((item) => item.sourceId === sourceId);
}

export async function saveSchema(schema: SchemaRecord) {
  const data = await readJson<SchemaRecord[]>(schemasPath, []);
  const next = data.filter((item) => item.schemaId !== schema.schemaId);
  next.push(schema);
  await writeJson(schemasPath, next);
}

export async function getSchema(schemaId: string): Promise<SchemaRecord | undefined> {
  const data = await readJson<SchemaRecord[]>(schemasPath, []);
  return data.find((item) => item.schemaId === schemaId);
}

export async function saveRuns(payload: unknown) {
  await writeJson(runsPath, payload);
}

export async function loadRuns<T>(fallback: T): Promise<T> {
  return readJson(runsPath, fallback);
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
