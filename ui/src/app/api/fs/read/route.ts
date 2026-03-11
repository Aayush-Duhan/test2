/**
 * GET /api/fs/read?path=...
 * Reads file content from the local filesystem.
 * Returns: { content: string, isBinary: boolean }
 */
import { NextRequest, NextResponse } from 'next/server';
import { readFile, stat } from 'node:fs/promises';
import { resolve, normalize } from 'node:path';

const PROJECT_ROOT = resolve(process.cwd(), '..', 'data');

function isBinaryContent(buffer: Buffer): boolean {
  // Check first 8KB for null bytes — a simple binary detection heuristic
  const checkLength = Math.min(buffer.length, 8192);
  for (let i = 0; i < checkLength; i++) {
    if (buffer[i] === 0) {
      return true;
    }
  }
  return false;
}

export async function GET(request: NextRequest) {
  try {
    const filePath = request.nextUrl.searchParams.get('path');

    if (!filePath) {
      return NextResponse.json(
        { error: 'path query parameter is required' },
        { status: 400 },
      );
    }

    const resolved = resolve(PROJECT_ROOT, normalize(filePath).replace(/^[/\\]+/, ''));

    // Security: ensure the resolved path is under PROJECT_ROOT
    if (!resolved.startsWith(PROJECT_ROOT)) {
      return NextResponse.json(
        { error: 'Path traversal not allowed' },
        { status: 403 },
      );
    }

    // Check file exists
    const fileStat = await stat(resolved);
    if (!fileStat.isFile()) {
      return NextResponse.json(
        { error: 'Not a file' },
        { status: 400 },
      );
    }

    const buffer = await readFile(resolved);
    const isBinary = isBinaryContent(buffer);

    return NextResponse.json({
      content: isBinary ? '' : buffer.toString('utf-8'),
      isBinary,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const status = (error as NodeJS.ErrnoException)?.code === 'ENOENT' ? 404 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}
