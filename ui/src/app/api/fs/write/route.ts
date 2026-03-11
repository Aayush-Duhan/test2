/**
 * POST /api/fs/write
 * Writes file content to the local filesystem.
 * Body: { filePath: string, content: string }
 */
import { NextRequest, NextResponse } from 'next/server';
import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve, normalize } from 'node:path';

// All file writes are scoped under the project's data directory
const PROJECT_ROOT = resolve(process.cwd(), '..', 'data');

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { filePath, content } = body as { filePath: string; content: string };

    if (!filePath || typeof content !== 'string') {
      return NextResponse.json(
        { error: 'filePath and content are required' },
        { status: 400 },
      );
    }

    // Resolve to absolute path under project root
    const resolved = resolve(PROJECT_ROOT, normalize(filePath).replace(/^[/\\]+/, ''));

    // Security: ensure the resolved path is under PROJECT_ROOT
    if (!resolved.startsWith(PROJECT_ROOT)) {
      return NextResponse.json(
        { error: 'Path traversal not allowed' },
        { status: 403 },
      );
    }

    // Ensure parent directory exists
    await mkdir(dirname(resolved), { recursive: true });

    // Write the file
    await writeFile(resolved, content, 'utf-8');

    return NextResponse.json({ success: true, path: resolved });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
