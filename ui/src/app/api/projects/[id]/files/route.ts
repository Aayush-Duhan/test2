import { NextRequest, NextResponse } from 'next/server';
import * as path from 'path';
import { promises as fs } from 'fs';
import type { Dirent } from 'fs';
import { getProject } from '@/lib/storage';

export const runtime = 'nodejs';

// Workspace: ../projects/<project.name>
const PROJECTS_DIR = path.resolve(process.cwd(), '..', 'projects');

// Optional safety: avoid huge reads (set to Infinity to disable)
const MAX_FILE_BYTES = 2_000_000; // 2MB

/**
 * ✅ ONLY show these three “virtual” roots in your UI.
 * Option A: output maps directly to converted/Output/SnowConvert
 *
 * UI sees:        maps to disk:
 *  - source   ->  source
 *  - output   ->  converted/Output/SnowConvert
 *  - reports  ->  converted/Reports
 */
const VIRTUAL_ROOTS: Record<string, string> = {
  source: 'source',
  output: path.posix.join('converted', 'Output', 'SnowConvert'),
  reports: path.posix.join('converted', 'Reports'),
};

type FileInfo =
  | { name: string; path: string; type: 'folder' }
  | { name: string; path: string; type: 'file'; content: string };

interface WritableFileInput {
  path: string;
  content: string;
}

/** Convert Windows "\" to "/" for consistent matching */
function toPosix(p: string) {
  return p.split(path.sep).join('/');
}

/** Strong path-inside check with separator boundary */
function isPathInside(rootAbs: string, targetAbs: string) {
  const root = path.resolve(rootAbs);
  const target = path.resolve(targetAbs);
  return target === root || target.startsWith(root + path.sep);
}

function splitVirtual(p: string) {
  const clean = toPosix(p || '').replace(/^\/+/, '');
  if (!clean) return { root: '', rest: '' };
  const [root, ...restParts] = clean.split('/');
  return { root, rest: restParts.join('/') };
}

/**
 * Convert a UI path like:
 *  - "" -> "" (virtual root)
 *  - "source/a.txt" -> "source/a.txt"
 *  - "output/x.txt" -> "converted/Output/SnowConvert/x.txt"
 *  - "reports/r1.json" -> "converted/Reports/r1.json"
 *
 * If root is not one of {source, output, reports}, returns null (blocked).
 */
function virtualToRealRelative(virtualRel: string): string | null {
  const { root, rest } = splitVirtual(virtualRel);

  // root listing allowed
  if (!root) return '';

  const mapped = VIRTUAL_ROOTS[root];
  if (!mapped) return null;

  return rest ? path.posix.join(mapped, rest) : mapped;
}

/**
 * Convert real relative paths back to virtual for returning to UI.
 * Example:
 *   "converted/Output/SnowConvert/file.txt" -> "output/file.txt"
 *   "source/main.ts" -> "source/main.ts"
 */
function realRelativeToVirtual(realRel: string): string {
  const realPosix = toPosix(realRel).replace(/^\/+/, '');

  for (const [virtualKey, mappedReal] of Object.entries(VIRTUAL_ROOTS)) {
    const mapped = toPosix(mappedReal).replace(/^\/+/, '');

    if (realPosix === mapped) return virtualKey;
    if (realPosix.startsWith(mapped + '/')) {
      return virtualKey + realPosix.slice(mapped.length);
    }
  }

  // Should not happen because we filter strictly, but safe fallback:
  return realPosix;
}

/** Ensure a given real-relative path is under one of the allowed real roots */
function isAllowedRealRelative(realRel: string): boolean {
  const realPosix = toPosix(realRel).replace(/^\/+/, '');
  if (!realPosix) return true; // root allowed

  return Object.values(VIRTUAL_ROOTS).some((mappedReal) => {
    const mapped = toPosix(mappedReal).replace(/^\/+/, '');
    return realPosix === mapped || realPosix.startsWith(mapped + '/');
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isWritableFileInput(value: unknown): value is WritableFileInput {
  return (
    isRecord(value) &&
    typeof value.path === 'string' &&
    typeof value.content === 'string'
  );
}

function getWritableFilePath(value: unknown): string {
  return isRecord(value) && typeof value.path === 'string'
    ? value.path
    : '(unknown)';
}

async function readTextFileSafe(absPath: string): Promise<string> {
  try {
    const stat = await fs.stat(absPath);
    if (stat.size > MAX_FILE_BYTES) {
      return `[File too large to preview (${stat.size} bytes). Limit is ${MAX_FILE_BYTES} bytes.]`;
    }
    return await fs.readFile(absPath, 'utf-8');
  } catch {
    return '[Unable to read file]';
  }
}

/**
 * Reads a directory (non-recursive), but only returns children that still lie
 * inside allowed roots, and returns paths in VIRTUAL form.
 *
 * ✅ Uses encoding:'utf8' to ensure Dirent.name is typed as string (fixes TS NonSharedBuffer errors)
 */
async function readDirectory(absDir: string, projectRootAbs: string): Promise<FileInfo[]> {
  const items: FileInfo[] = [];

  let entries: Dirent[];
  try {
    entries = await fs.readdir(absDir, { withFileTypes: true, encoding: 'utf8' });
  } catch {
    return items;
  }

  for (const entry of entries) {
    const name = entry.name; // string
    const fullAbs = path.join(absDir, name);

    const realRel = path.relative(projectRootAbs, fullAbs);

    // Enforce allowlist (hard filter)
    if (!isAllowedRealRelative(realRel)) continue;

    const virtualRel = realRelativeToVirtual(realRel);

    if (entry.isDirectory()) {
      items.push({ name, path: virtualRel, type: 'folder' });
    } else if (entry.isFile()) {
      const content = await readTextFileSafe(fullAbs);
      items.push({ name, path: virtualRel, type: 'file', content });
    }
  }

  return items;
}

async function getParamsId(
  params: { id: string } | Promise<{ id: string }>
): Promise<string> {
  const resolved = await params;
  return resolved.id;
}

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } | Promise<{ id: string }> }
) {
  const id = await getParamsId(params);

  const project = await getProject(id);
  if (!project) {
    return NextResponse.json({ error: 'Project not found in registry' }, { status: 404 });
  }

  const projectRootAbs = path.join(PROJECTS_DIR, project.name);

  // Ensure project root exists
  try {
    const stat = await fs.stat(projectRootAbs);
    if (!stat.isDirectory()) {
      return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
    }
  } catch {
    return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
  }

  const virtualPath = request.nextUrl.searchParams.get('path') || '';

  /**
   * Root listing:
   * show ONLY [source, output, reports] as folders (if they exist).
   * Note: output exists only if converted/Output/SnowConvert exists.
   */
  if (!virtualPath) {
    const roots: FileInfo[] = [];

    for (const [virtualKey, realRel] of Object.entries(VIRTUAL_ROOTS)) {
      const abs = path.join(projectRootAbs, ...toPosix(realRel).split('/'));
      try {
        const s = await fs.stat(abs);
        if (s.isDirectory()) {
          roots.push({ name: virtualKey, path: virtualKey, type: 'folder' });
        }
      } catch {
        // If a folder doesn’t exist yet, just don’t show it.
      }
    }

    return NextResponse.json({
      type: 'directory',
      path: '',
      items: roots,
    });
  }

  // Convert virtual -> real (block unknown roots like .scai/settings/results/converted/Logs)
  const realRel = virtualToRealRelative(virtualPath);
  if (realRel === null) {
    return NextResponse.json({ error: 'Path not allowed' }, { status: 404 });
  }

  // Absolute target path
  const targetAbs = path.join(projectRootAbs, ...toPosix(realRel).split('/'));

  // Must remain inside project folder
  if (!isPathInside(projectRootAbs, targetAbs)) {
    return NextResponse.json({ error: 'Invalid path' }, { status: 400 });
  }

  // Must remain inside allowed roots
  if (!isAllowedRealRelative(realRel)) {
    return NextResponse.json({ error: 'Path not allowed' }, { status: 404 });
  }

  let stat;
  try {
    stat = await fs.stat(targetAbs);
  } catch {
    return NextResponse.json({ error: 'Path not found' }, { status: 404 });
  }

  if (stat.isDirectory()) {
    const items = await readDirectory(targetAbs, projectRootAbs);
    return NextResponse.json({
      type: 'directory',
      path: virtualPath,
      items,
    });
  }

  if (stat.isFile()) {
    const content = await readTextFileSafe(targetAbs);
    return NextResponse.json({
      type: 'file',
      path: virtualPath,
      name: path.basename(targetAbs),
      content,
    });
  }

  return NextResponse.json({ error: 'Unknown file type' }, { status: 400 });
}

export async function POST(
  request: NextRequest,
  { params }: { params: { id: string } | Promise<{ id: string }> }
) {
  const id = await getParamsId(params);

  const project = await getProject(id);
  if (!project) {
    return NextResponse.json({ error: 'Project not found in registry' }, { status: 404 });
  }

  const projectRootAbs = path.join(PROJECTS_DIR, project.name);

  // Ensure project root exists
  try {
    const stat = await fs.stat(projectRootAbs);
    if (!stat.isDirectory()) {
      return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
    }
  } catch {
    return NextResponse.json({ error: 'Project directory not found' }, { status: 404 });
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON body' }, { status: 400 });
  }

  const files = isRecord(body) ? body.files : undefined;
  if (!Array.isArray(files)) {
    return NextResponse.json(
      { error: 'Body must be { files: Array<{ path, content }> }' },
      { status: 400 }
    );
  }

  const results: { path: string; success: boolean; error?: string }[] = [];

  for (const file of files) {
    try {
      if (!isWritableFileInput(file)) {
        results.push({
          path: getWritableFilePath(file),
          success: false,
          error: 'Invalid file entry. Expected { path: string; content: string }',
        });
        continue;
      }

      // Convert virtual -> real (block unknown roots)
      const realRel = virtualToRealRelative(file.path);
      if (realRel === null) {
        results.push({ path: file.path, success: false, error: 'Path not allowed' });
        continue;
      }

      // Must be within allowed roots
      if (!isAllowedRealRelative(realRel)) {
        results.push({ path: file.path, success: false, error: 'Path not allowed' });
        continue;
      }

      const targetAbs = path.join(projectRootAbs, ...toPosix(realRel).split('/'));

      // Must remain inside project
      if (!isPathInside(projectRootAbs, targetAbs)) {
        results.push({ path: file.path, success: false, error: 'Invalid path' });
        continue;
      }

      await fs.mkdir(path.dirname(targetAbs), { recursive: true });
      await fs.writeFile(targetAbs, file.content, 'utf-8');

      results.push({ path: file.path, success: true });
    } catch (error) {
      results.push({
        path: getWritableFilePath(file),
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  }

  return NextResponse.json({ results });
}
