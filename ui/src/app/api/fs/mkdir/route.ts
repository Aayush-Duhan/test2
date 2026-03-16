/**
 * POST /api/fs/mkdir
 * Creates directories recursively on the local filesystem.
 * Body: { dirPath: string }
 */
import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "node:fs";
import { resolveSafePath } from "@/lib/fs-guard";

export const runtime = "nodejs";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { dirPath } = body as { dirPath: string };

    if (!dirPath) {
      return NextResponse.json({ error: "dirPath is required" }, { status: 400 });
    }

    const { absPath } = await resolveSafePath(dirPath, {
      mustExist: false,
      allowDir: true,
      allowFile: false,
    });

    await fs.mkdir(absPath, { recursive: true });

    return NextResponse.json({ success: true, path: absPath });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
