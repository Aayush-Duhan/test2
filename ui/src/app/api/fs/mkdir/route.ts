/**
 * POST /api/fs/mkdir
 * Creates directories recursively on the local filesystem.
 * Body: { dirPath: string }
 */
import { NextRequest, NextResponse } from 'next/server';
import { mkdir } from 'node:fs/promises';
import { resolve, normalize } from 'node:path';

const PROJECT_ROOT = resolve(process.cwd(), '..', 'data');

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { dirPath } = body as { dirPath: string };

    if (!dirPath) {
      return NextResponse.json(
        { error: 'dirPath is required' },
        { status: 400 },
      );
    }

    const resolved = resolve(PROJECT_ROOT, normalize(dirPath).replace(/^[/\\]+/, ''));

    // Security: ensure the resolved path is under PROJECT_ROOT
    if (!resolved.startsWith(PROJECT_ROOT)) {
      return NextResponse.json(
        { error: 'Path traversal not allowed' },
        { status: 403 },
      );
    }

    await mkdir(resolved, { recursive: true });

    return NextResponse.json({ success: true, path: resolved });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
